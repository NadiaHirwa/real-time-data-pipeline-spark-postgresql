# Error Handling and Recovery

Error handling covers what happens to a specific bad thing right now. Recovery covers what happens after a bigger failure (a crash, a restart). These are documented separately because they operate at different levels: one is a per-record decision, the other is a whole-system behavior.

## Error Handling

| Error | Detected By | Action |
|---|---|---|
| Invalid event_type (not view/purchase) | tag_validation_result() in spark_streaming.py | Row routed to rejected_events, tagged invalid_event_type |
| Negative price | tag_validation_result() | Routed to rejected_events, tagged invalid_or_negative_price |
| Zero or negative quantity | tag_validation_result() | Routed to rejected_events, tagged invalid_or_zero_quantity |
| Missing/unparseable user_id or product_id | try_cast() producing null, then tag_validation_result() | Routed to rejected_events, tagged accordingly |
| Malformed or far-future event_timestamp | try_cast() / timestamp comparison | Routed to rejected_events, tagged accordingly |
| Duplicate event_id within one micro-batch | dropDuplicates(["event_id"]) | Silently dropped from the valid stream (not logged as an individual reject, since it's not a data quality issue, just a within-batch duplicate) |
| Duplicate event_id across separate micro-batches or runs | PostgreSQL's UNIQUE constraint on events.event_id | ON CONFLICT DO NOTHING in the upsert - the row is silently skipped, never causing a crash |
| PostgreSQL temporarily unreachable | psycopg2.connect() raising an exception inside foreachBatch | The exception propagates and fails that micro-batch; because file archiving only happens after both writes succeed, the source files remain in data/incoming/ and are retried on the next trigger |
| Corrupted or unreadable CSV file | Spark's CSV reader | Not explicitly tested in this project; Spark's default file-source behavior applies (see risks_and_limitations.md) |

## Recovery

- **Spark job crash or manual restart**: on restart, spark.readStream resumes from the checkpoint directory (checkpoint/), re-reading only files not yet marked processed. No manual intervention is required.
- **PostgreSQL restart or temporary unavailability**: the next scheduled micro-batch trigger will attempt to connect again; no explicit retry-with-backoff logic is implemented beyond Spark's own batch retry behavior on task failure.
- **Manual full reset** (documented in user_guide.md): python main.py clean clears generated/archived CSVs and the checkpoint directory. This does NOT touch database rows - a full reset including stored data requires manually running TRUNCATE TABLE events, rejected_events; in PostgreSQL.

## What Is Explicitly Not Handled

- No automatic retry-with-backoff for a persistently unreachable database (a batch will simply fail and be retried on the next natural trigger, with no exponential delay)
- No dead-letter mechanism beyond the rejected_events table itself - there is no separate handling for "a row that fails validation repeatedly" versus "a row that failed validation once"
- No alerting (email, Slack, etc.) on repeated failures - failures are visible only in the log file and console output