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
import uuid
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
    """
    Create the SparkSession, wiring in the PostgreSQL JDBC driver via
    Maven coordinates rather than a manually-downloaded local .jar
    file. The original version hardcoded a Windows-specific path
    (file:///C:/spark-jars/...), which does not exist on Linux CI
    runners or any other machine - spark.jars.packages tells Spark to
    fetch (and cache) the driver from Maven automatically, working
    identically across Windows, Linux, and CI.
    """
    return (
        SparkSession.builder
        .appName("EcommerceEventStreaming")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
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


def write_valid_to_postgres(valid_df: DataFrame, jdbc_url: str, run_id: str, batch_id: int) -> None:
    """
    Bulk-write-then-merge upsert (see docs/engineering_decisions.md
    for the full reasoning behind this replacing the earlier
    foreachPartition/psycopg2 approach).

    Step 1 (this function): Spark's own JDBC bulk writer appends
    valid_df into staging_events, tagged with run_id + batch_id so
    the merge step below can target exactly these rows. This runs
    as a normal Spark write - no foreachPartition, no worker-side
    custom-module-import risk.
    """
    (
        valid_df
        .withColumn("run_id", F.lit(run_id))
        .withColumn("batch_id", F.lit(batch_id))
        .select(
            "run_id", "batch_id", "event_id", "user_id", "product_id",
            "event_type", "price", "quantity", "category", "event_timestamp",
        )
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "staging_events")
        .option("user", config.DB_USER)
        .option("password", config.DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


# Fixed, arbitrary key for this pipeline's advisory lock. Postgres
# advisory locks are keyed by integer, not name - this specific
# number has no meaning beyond "the one this pipeline always uses,"
# chosen once and never reused for anything else in this database.
STAGING_MERGE_LOCK_KEY = 918273645


def merge_staging_to_events(run_id: str, batch_id: int) -> None:
    """
    Step 2: move this batch's rows from staging_events into events,
    upserting with ON CONFLICT DO NOTHING, then delete the staged
    rows. Wrapped in a Postgres advisory lock so two runs sharing the
    same staging table (e.g. an overlapping restart) can never merge
    concurrently and interleave each other's rows.

    event_id::uuid casts explicitly - staging_events.event_id is TEXT
    (staging intentionally has no constraints, see postgres_setup.sql),
    but events.event_id is UUID; Postgres will not implicitly convert
    TEXT to UUID inside an INSERT...SELECT even when the text is a
    valid UUID string.

    If the INSERT/DELETE fails, the transaction is explicitly rolled
    back BEFORE attempting to release the advisory lock - Postgres
    refuses to run ANY further command, including pg_advisory_unlock,
    on a transaction that already failed, so skipping the rollback
    here would leave the lock held indefinitely on any real error.
    """
    conn = psycopg2.connect(
        host=config.DB_HOST, port=config.DB_PORT, dbname=config.DB_NAME,
        user=config.DB_USER, password=config.DB_PASSWORD,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s);", (STAGING_MERGE_LOCK_KEY,))
            conn.commit()

            try:
                cur.execute(
                    """
                    INSERT INTO events
                        (event_id, user_id, product_id, event_type, price, quantity, category, event_timestamp)
                    SELECT DISTINCT ON (event_id)
                        event_id::uuid, user_id, product_id, event_type, price, quantity, category, event_timestamp
                    FROM staging_events
                    WHERE run_id = %s AND batch_id = %s
                    ORDER BY event_id
                    ON CONFLICT (event_id) DO NOTHING;
                    """,
                    (run_id, batch_id),
                )
                cur.execute(
                    "DELETE FROM staging_events WHERE run_id = %s AND batch_id = %s;",
                    (run_id, batch_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s);", (STAGING_MERGE_LOCK_KEY,))
                conn.commit()
    finally:
        conn.close()


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


def process_batch(batch_df: DataFrame, batch_id: int, run_id: str) -> None:
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
        write_valid_to_postgres(valid_df, config.JDBC_URL, run_id, batch_id)
        merge_staging_to_events(run_id, batch_id)
        logger.info("Batch %d: staged and merged %d valid rows into events", batch_id, valid_count)

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

    run_id = str(uuid.uuid4())
    logger.info("Streaming run starting with run_id=%s", run_id)

    stream_df = read_incoming_stream(spark)

    query = (
        stream_df.writeStream
        .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, run_id))
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