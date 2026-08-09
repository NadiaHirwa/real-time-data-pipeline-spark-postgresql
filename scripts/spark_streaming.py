"""
spark_streaming.py

Spark Structured Streaming job: watches data/incoming/ for new CSV
files, validates and transforms each micro-batch, writes valid rows
to PostgreSQL, rejects invalid rows to rejected_events, and archives
processed source files.

STAGE 3 (current): consolidates Stage 2's validation logic into a
SINGLE streaming query driven by foreachBatch(), with a real
checkpoint location. Stage 2 ran two independent streaming queries
over the same source, which duplicated work and caused a messy,
error-prone shutdown - see docs/engineering_decisions.md for the full
explanation. Still prints valid/rejected samples to console; the
actual Postgres write comes in Stage 4.
"""

import sys
from pathlib import Path

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
])


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
    """Read the incoming folder as a stream of raw (all-string) events."""
    return (
        spark.readStream
        .option("header", "true")
        .schema(EVENT_SCHEMA)
        .csv(str(config.INCOMING_DIR))
    )


def cast_and_normalize(df: DataFrame) -> DataFrame:
    """Safely cast and normalize raw columns - see Stage 2 docstring for full reasoning."""
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
    """Tag each row with its rejection_reason (or null if valid) - see docs/data_contract.md."""
    return df.withColumn(
        "rejection_reason",
        F.when(F.col("user_id").isNull(), F.lit("missing_or_invalid_user_id"))
         .when(F.col("product_id").isNull(), F.lit("missing_or_invalid_product_id"))
         .when(~F.col("event_type").isin(config.ALLOWED_EVENT_TYPES), F.lit("invalid_event_type"))
         .when(F.col("price").isNull() | (F.col("price") < 0), F.lit("invalid_or_negative_price"))
         .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_or_zero_quantity"))
         .when(F.col("event_timestamp").isNull(), F.lit("unparseable_timestamp"))
         .when(F.col("event_timestamp") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES"),
               F.lit("future_timestamp"))
         .otherwise(F.lit(None)),
    )


def split_valid_and_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split one tagged micro-batch into (valid_df, rejected_df). See Stage 2 docstring."""
    valid_df = (
        df.filter(F.col("rejection_reason").isNull())
        .dropDuplicates(["event_id"])
        .drop("rejection_reason")
    )
    rejected_df = df.filter(F.col("rejection_reason").isNotNull())
    return valid_df, rejected_df


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called once per micro-batch by foreachBatch(). batch_df here is an
    ORDINARY (non-streaming) DataFrame - all of Spark's normal batch
    operations, including .count() and .show(), work on it freely,
    unlike on a streaming DataFrame.
    """
    cast_df = cast_and_normalize(batch_df)
    tagged_df = tag_validation_result(cast_df)
    valid_df, rejected_df = split_valid_and_rejected(tagged_df)

    # .cache() so the two .count() calls below and the eventual
    # Postgres write (Stage 4) don't each independently recompute
    # the same transformations from scratch.
    valid_df.cache()
    rejected_df.cache()

    valid_count = valid_df.count()
    rejected_count = rejected_df.count()

    logger.info(
        "Batch %d: %d valid, %d rejected", batch_id, valid_count, rejected_count
    )

    if valid_count > 0:
        print(f"--- Batch {batch_id}: VALID sample ---")
        valid_df.show(5, truncate=False)

    if rejected_count > 0:
        print(f"--- Batch {batch_id}: REJECTED sample ---")
        rejected_df.show(5, truncate=False)

    valid_df.unpersist()
    rejected_df.unpersist()


def run_stage3(spark: SparkSession) -> None:
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

    logger.info("Stage 3 streaming query started. Checkpoint: %s", config.CHECKPOINT_DIR)
    query.awaitTermination()


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        run_stage3(spark)
    except KeyboardInterrupt:
        logger.info("Streaming job stopped by user.")
    finally:
        spark.stop()