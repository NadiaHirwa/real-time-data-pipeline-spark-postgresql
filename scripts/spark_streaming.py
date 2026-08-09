"""
spark_streaming.py

Spark Structured Streaming job: watches data/incoming/ for new CSV
files, validates and transforms each micro-batch, writes valid rows
to PostgreSQL, rejects invalid rows to rejected_events, and archives
processed source files.

STAGE 1 (current): schema definition + basic streaming read, printed
to the console. No validation or Postgres writes yet - this stage
exists purely to prove Spark is correctly detecting and reading new
files as they land, before adding any further complexity.
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

sys.path.append(str(Path(__file__).resolve().parent))
import config
from monitoring_logger import get_logger

logger = get_logger(__name__)

# Matches docs/data_dictionary.md exactly. Declared explicitly because
# Structured Streaming cannot infer a schema from files that haven't
# arrived yet (see docs/engineering_decisions.md).
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),      # read as string first;
    StructField("product_id", StringType(), True),   # cast + validated in Stage 2,
    StructField("event_type", StringType(), True),   # so a malformed value becomes
    StructField("price", StringType(), True),         # a REJECT, not a silent null
    StructField("quantity", StringType(), True),      # from a failed inline cast
    StructField("category", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])


def build_spark_session() -> SparkSession:
    """
    Create the SparkSession, wiring in the PostgreSQL JDBC driver via
    spark.jars - required for the JVM to be able to talk to Postgres
    at all, independent of the psycopg2 package used by database.py.
    """
    jdbc_jar_path = "file:///C:/spark-jars/postgresql-42.7.13.jar"

    return (
        SparkSession.builder
        .appName("EcommerceEventStreaming")
        .master("local[*]")
        .config("spark.jars", jdbc_jar_path)
        .getOrCreate()
    )


def run_stage1(spark: SparkSession) -> None:
    """Read the incoming folder as a stream and print each micro-batch to console."""
    config.ensure_directories()

    stream_df = (
        spark.readStream
        .option("header", "true")
        .schema(EVENT_SCHEMA)
        .csv(str(config.INCOMING_DIR))
    )

    query = (
        stream_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime=f"{config.TRIGGER_INTERVAL_SECONDS} seconds")
        .start()
    )

    logger.info("Stage 1 streaming query started. Watching %s", config.INCOMING_DIR)
    query.awaitTermination()


if __name__ == "__main__":
    spark = build_spark_session()
    try:
        run_stage1(spark)
    except KeyboardInterrupt:
        logger.info("Streaming job stopped by user.")
    finally:
        spark.stop()