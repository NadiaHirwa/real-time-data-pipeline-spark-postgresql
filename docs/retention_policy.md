# Retention Policy

Documents how long each category of data/file is kept, and when it should be removed. As noted in risks_and_limitations.md, this is a documented POLICY, not an implemented automated process - no code in this project currently enforces these retention periods automatically.

## Source Files

| Location | Retention | Rationale |
|---|---|---|
| data/incoming/ | Until processed (typically seconds); EXCEPTION: zero-row files (empty or header-only CSV) are read successfully but never archived, so they remain here indefinitely - see risks_and_limitations.md | Files are moved out immediately upon successful processing for any file that produces at least one row; this folder should not accumulate long-term under normal operation |
| data/processed_archive/ | Not automatically deleted | Kept as an audit trail of every file the pipeline has ever ingested; a real deployment would need a defined retention window (e.g. 30-90 days) with automated cleanup - not implemented here |
| data/rejected/ | N/A - this project writes rejected data to the rejected_events table instead of CSV files (see engineering_decisions.md) | - |

## Database

| Table | Retention | Rationale |
|---|---|---|
| events | Not automatically deleted | Represents the core, valid dataset; no expiry logic exists or was requested for this project's scope |
| rejected_events | Not automatically deleted | Kept for ongoing data quality analysis; a real deployment might purge entries older than a fixed window once they've been reviewed |
| staging_events | Should always be empty between batches - a merge deletes its own rows immediately after moving them into events | Non-empty rows found here indicate an orphaned batch (a merge that started but failed partway through) - see risks_and_limitations.md. No automated cleanup exists for orphaned rows |
| stream_metrics | Not automatically deleted | One row per micro-batch, across all runs; grows steadily but slowly (one row per trigger, not per event) - no retention policy defined since volume is low relative to events/rejected_events |

## Logs

| File | Retention | Rationale |
|---|---|---|
| logs/pipeline.log | Not automatically rotated or deleted | Grows indefinitely across all runs; a production deployment would need log rotation (e.g. via Python's RotatingFileHandler, or an external tool) - not implemented here, listed in future_improvements.md |

## Checkpoint

| Location | Retention | Rationale |
|---|---|---|
| checkpoint/ | Persists across restarts by design - this is what enables recovery | Should only be cleared deliberately (via python main.py clean) when a genuinely fresh start is wanted, since clearing it discards Spark's record of which files have already been processed |

## Manual Reset Procedure

**Only run this while the streaming job is stopped.** Running python main.py clean while python main.py stream is still active deletes the checkpoint directory out from under a live query - this was observed during development (see user_guide.md) and caused the streaming job to stop picking up new files correctly.

For a genuinely clean local test environment:

1. Stop the streaming job (Ctrl+C) and confirm it has fully exited
2. python main.py clean - removes generated/archived CSVs and clears the checkpoint
3. In PostgreSQL: TRUNCATE TABLE events, rejected_events, staging_events, stream_metrics; - clears stored data (not automated; must be run manually, deliberately, since this is a destructive action)