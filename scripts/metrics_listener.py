"""
metrics_listener.py

A Spark StreamingQueryListener that writes one row to stream_metrics
per micro-batch, using Spark's own internal timing rather than
manually reading log timestamps (see docs/performance_methodology.md
for the earlier, more labor-intensive approach this supersedes).

StreamingQueryListener callbacks run on the DRIVER, not inside a
worker subprocess - unlike make_write_valid_partition() in
spark_streaming.py, normal imports of project modules (config) are
genuinely safe here. See docs/engineering_decisions.md's standing
rule on this distinction.
"""

import sys
from pathlib import Path

import psycopg2
from pyspark.sql.streaming import StreamingQueryListener

sys.path.append(str(Path(__file__).resolve().parent))
import config
from monitoring_logger import get_logger

logger = get_logger(__name__)


class MetricsListener(StreamingQueryListener):
    """
    Registered once via spark.streams.addListener(MetricsListener(run_id))
    before starting the query. onQueryProgress fires automatically
    after every completed micro-batch.
    """

    def __init__(self, run_id: str):
        self.run_id = run_id

    def onQueryStarted(self, event):
        logger.info("Metrics listener attached to query %s", event.id)

    def onQueryProgress(self, event):
        progress = event.progress
        duration_map = progress.durationMs or {}

        try:
            conn = psycopg2.connect(
                host=config.DB_HOST, port=config.DB_PORT, dbname=config.DB_NAME,
                user=config.DB_USER, password=config.DB_PASSWORD,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO stream_metrics
                            (run_id, query_id, batch_id, batch_timestamp,
                             num_input_rows, input_rows_per_second,
                             processed_rows_per_second, batch_duration_ms,
                             add_batch_ms, get_batch_ms, trigger_execution_ms)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, batch_id) DO NOTHING;
                        """,
                        (
                            self.run_id,
                            str(progress.id),
                            progress.batchId,
                            progress.timestamp,
                            progress.numInputRows,
                            progress.inputRowsPerSecond,
                            progress.processedRowsPerSecond,
                            duration_map.get("triggerExecution"),
                            duration_map.get("addBatch"),
                            duration_map.get("getBatch"),
                            duration_map.get("triggerExecution"),
                        ),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception:
            # A metrics-logging failure should never take down the
            # actual streaming pipeline - log it and move on, rather
            # than letting an observability feature become a new
            # source of pipeline downtime.
            logger.exception("Failed to write batch metrics for batch_id=%s", progress.batchId)

    def onQueryTerminated(self, event):
        logger.info("Streaming query terminated: %s", event.id)