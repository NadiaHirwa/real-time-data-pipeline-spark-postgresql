"""
spark_streaming.py

Spark Structured Streaming job: watches data/incoming/ for new CSV
files, validates and transforms each micro-batch, writes valid rows
to PostgreSQL, rejects invalid rows to rejected_events, and archives
processed source files.

STAGE 4 (current): adds the real PostgreSQL writes and file archiving
on top of Stage 3's proven single-query foreachBatch structure.

Valid rows are written via psycopg2's execute_values() with
ON CONFLICT DO NOTHING, NOT Spark's standard JDBC writer - Spark's
JDBC writer has no upsert support, and would crash the entire batch
on a single duplicate event_id (a real possibility across batches,
per docs/engineering_decisions.md's at-least-once delivery note).
Rejected rows have no such constraint and use the standard JDBC
writer directly.
"""

import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

sys.path.append(str(Path(__file__).resolve().parent))
import config
from monitoring_logger import get_logger

logger = get_logger(__name__)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("category", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

CORRUPT_RECORD_COLUMN = "_corrupt_record"


def build_spark_session() -> SparkSession:
    """Create the SparkSession, wiring in the PostgreSQL JDBC driver."""
    jdbc_jar_path = "file:///C:/spark-jars/postgresql-42.7.13.jar"
    return (
        SparkSession.builder
        .appName("EcommerceEventStreaming")
        .master("local[*]")
        .config("spark.jars", jdbc_jar_path)
        .getOrCreate()
    )


def read_incoming_stream(spark: SparkSession) -> DataFrame:
    """
    Read the incoming folder as a stream, tagging each row with its
    source file path via input_file_name() - needed later so we know
    exactly which files can be safely archived once a batch succeeds.

    columnNameOfCorruptRecord + PERMISSIVE mode: a structurally broken
    CSV row (wrong field count, unparseable structure) is not silently
    dropped or partially nulled - its raw text is captured in
    _corrupt_record, which tag_validation_result() checks first,
    before any of the normal field-level validation rules run.
    """
    return (
        spark.readStream
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .schema(EVENT_SCHEMA)
        .csv(str(config.INCOMING_DIR))
        .withColumn("_source_file", F.input_file_name())
    )


def cast_and_normalize(df: DataFrame) -> DataFrame:
    """Safely cast and normalize raw columns."""
    return (
        df
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("user_id", F.trim(F.col("user_id")).try_cast("int"))
        .withColumn("product_id", F.trim(F.col("product_id")).try_cast("int"))
        .withColumn("price", F.trim(F.col("price")).try_cast("double"))
        .withColumn("quantity", F.trim(F.col("quantity")).try_cast("int"))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("event_timestamp", F.col("event_timestamp").try_cast("timestamp"))
    )


def tag_validation_result(df: DataFrame) -> DataFrame:
    """Tag each row with its rejection_reason (or null if valid)."""
    return df.withColumn(
        "rejection_reason",
        F.when(F.col(CORRUPT_RECORD_COLUMN).isNotNull(), F.lit("malformed_csv_row"))
         .when(~F.col("event_id").rlike(config.UUID_PATTERN), F.lit("invalid_event_id_format"))
         .when(F.col("user_id").isNull(), F.lit("missing_or_invalid_user_id"))
         .when(F.col("product_id").isNull(), F.lit("missing_or_invalid_product_id"))
         .when(~F.col("event_type").isin(config.ALLOWED_EVENT_TYPES), F.lit("invalid_event_type"))
         .when(F.col("price").isNull() | (F.col("price") < 0), F.lit("invalid_or_negative_price"))
         .when(F.col("price") > config.MAX_PRICE, F.lit("price_exceeds_maximum"))
         .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_or_zero_quantity"))
         .when(F.col("quantity") > config.MAX_QUANTITY, F.lit("quantity_exceeds_maximum"))
         .when(F.col("event_timestamp").isNull(), F.lit("unparseable_timestamp"))
         .when(F.col("event_timestamp") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"),
               F.lit("future_timestamp"))
         .otherwise(F.lit(None)),
    )


def split_valid_and_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split one tagged micro-batch into (valid_df, rejected_df)."""
    valid_df = (
        df.filter(F.col("rejection_reason").isNull())
        .dropDuplicates(["event_id"])
        .drop("rejection_reason")
    )
    rejected_df = df.filter(F.col("rejection_reason").isNotNull())
    return valid_df, rejected_df


def make_write_valid_partition(db_host: str, db_port: str, db_name: str, db_user: str, db_password: str):
    """
    Returns a self-contained partition-writer function with the DB
    credentials captured as plain strings in its closure, and its own
    inline retry/classification logic.

    IMPORTANT: this function must never reference a custom project
    module (config, retry, errors, monitoring_logger) by name inside
    its body. Functions passed to foreachPartition run in SEPARATE
    worker subprocesses on Windows local[*], which do not inherit the
    driver's sys.path.append() calls - a worker trying to "import
    retry" (or any other project module) fails with ModuleNotFoundError,
    which crashes the worker process outright rather than raising a
    catchable Python exception. This exact bug was hit twice: once
    with `config`, once with `retry` - see docs/engineering_decisions.md.
    Only genuinely pip-installed packages (psycopg2, random, time) and
    plain captured values are safe to use here.
    """
    import random
    import time

    RETRYABLE_SQLSTATE_PREFIXES = ("08", "53")
    RETRYABLE_SQLSTATES = {"40001", "40P01", "55P03", "57P01", "57P02", "57P03"}
    MAX_ATTEMPTS = 5
    BASE_DELAY = 0.5
    MAX_DELAY = 15.0

    def _is_transient(exc) -> bool:
        sqlstate = getattr(exc, "pgcode", None)
        if sqlstate:
            return sqlstate in RETRYABLE_SQLSTATES or sqlstate.startswith(RETRYABLE_SQLSTATE_PREFIXES)
        text = str(exc).lower()
        markers = ("connection refused", "connection reset", "could not connect",
                   "starting up", "terminating connection", "deadlock detected",
                   "too many clients", "timeout", "broken pipe")
        return any(m in text for m in markers)

    def write_valid_partition(rows) -> None:
        rows = list(rows)
        if not rows:
            return

        def _write():
            conn = psycopg2.connect(
                host=db_host, port=db_port, dbname=db_name,
                user=db_user, password=db_password,
            )
            try:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO events
                            (event_id, user_id, product_id, event_type, price, quantity, category, event_timestamp)
                        VALUES %s
                        ON CONFLICT (event_id) DO NOTHING;
                        """,
                        [
                            (r.event_id, r.user_id, r.product_id, r.event_type,
                             r.price, r.quantity, r.category, r.event_timestamp)
                            for r in rows
                        ],
                    )
                conn.commit()
            finally:
                conn.close()

        last_exc = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                _write()
                return
            except Exception as exc:
                if not _is_transient(exc):
                    raise
                last_exc = exc
                if attempt == MAX_ATTEMPTS:
                    break
                backoff = min(MAX_DELAY, BASE_DELAY * (2 ** (attempt - 1)))
                time.sleep(random.uniform(0.0, backoff))

        raise last_exc

    return write_valid_partition


def write_valid_to_postgres(valid_df: DataFrame) -> None:
    """
    Distribute the upsert across partitions - see make_write_valid_partition().

    repartition(4) caps the number of psycopg2 connections opened per
    batch to 4, regardless of Spark's default parallelism. Without
    this, local[*] splits even a small batch across ~20 partitions,
    each paying its own connection-setup cost - the exact issue
    diagnosed and fixed earlier (see performance_metrics.md); it was
    accidentally dropped when this function was rewritten to fix the
    retry-logic import bug, and is restored here.
    """
    writer = make_write_valid_partition(
        config.DB_HOST, config.DB_PORT, config.DB_NAME, config.DB_USER, config.DB_PASSWORD,
    )
    (
        valid_df
        .repartition(4)
        .select(
            "event_id", "user_id", "product_id", "event_type",
            "price", "quantity", "category", "event_timestamp",
        )
        .foreachPartition(writer)
    )


def write_rejected_to_postgres(rejected_df: DataFrame, jdbc_url: str) -> None:
    """
    rejected_events has no unique constraint, so the standard Spark
    JDBC writer (simple append, no upsert needed) is safe here.
    """
    (
        rejected_df
        .select(
            "event_id", "user_id", "product_id", "event_type",
            "price", "quantity", "category", "event_timestamp", "rejection_reason",
        )
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "rejected_events")
        .option("user", config.DB_USER)
        .option("password", config.DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def archive_source_files(df: DataFrame) -> None:
    """
    Move every source CSV referenced in this batch from incoming/ to
    processed_archive/, so the landing folder doesn't grow unbounded
    and files are never reprocessed on a restart (see
    docs/retention_policy.md).
    """
    source_files = [row["_source_file"] for row in df.select("_source_file").distinct().collect()]
    for file_uri in source_files:
        file_path = Path(file_uri.replace("file:///", "").replace("file:", ""))
        if file_path.exists():
            destination = config.ARCHIVE_DIR / file_path.name
            file_path.rename(destination)
            logger.info("Archived %s", file_path.name)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Called once per micro-batch by foreachBatch()."""
    cast_df = cast_and_normalize(batch_df)
    tagged_df = tag_validation_result(cast_df)
    valid_df, rejected_df = split_valid_and_rejected(tagged_df)

    valid_df.cache()
    rejected_df.cache()

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    logger.info("Batch %d: %d valid, %d rejected", batch_id, valid_count, rejected_count)

    if valid_count > 0:
        write_valid_to_postgres(valid_df)
        logger.info("Batch %d: wrote %d valid rows to events", batch_id, valid_count)

    if rejected_count > 0:
        write_rejected_to_postgres(rejected_df, config.JDBC_URL)
        logger.info("Batch %d: wrote %d rejected rows to rejected_events", batch_id, rejected_count)

    # Archive only after both writes succeed - if either write above
    # raised an exception, execution never reaches here, and the
    # source files remain in incoming/ to be retried on the next
    # trigger (or the next restart, via checkpoint recovery).
    archive_source_files(batch_df)

    valid_df.unpersist()
    rejected_df.unpersist()


def run(spark: SparkSession) -> None:
    """Read the stream and process each micro-batch through process_batch()."""
    config.ensure_directories()

    stream_df = read_incoming_stream(spark)

    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(config.CHECKPOINT_DIR))
        .trigger(processingTime=f"{config.TRIGGER_INTERVAL_SECONDS} seconds")
        .start()
    )

    logger.info("Streaming query started. Checkpoint: %s", config.CHECKPOINT_DIR)
    query.awaitTermination()


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        run(spark)
    except KeyboardInterrupt:
        logger.info("Streaming job stopped by user.")
    finally:
        spark.stop()