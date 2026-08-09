# Retention Policy

Documents how long each category of data/file is kept, and when it should be removed. As noted in risks_and_limitations.md, this is a documented POLICY, not an implemented automated process - no code in this project currently enforces these retention periods automatically.

## Source Files

| Location | Retention | Rationale |
|---|---|---|
| data/incoming/ | Until processed (typically seconds) | Files are moved out immediately upon successful processing; this folder should never accumulate long-term |
| data/processed_archive/ | Not automatically deleted | Kept as an audit trail of every file the pipeline has ever ingested; a real deployment would need a defined retention window (e.g. 30-90 days) with automated cleanup - not implemented here |
| data/rejected/ | N/A - this project writes rejected data to the rejected_events table instead of CSV files (see engineering_decisions.md) | - |

## Database

| Table | Retention | Rationale |
|---|---|---|
| events | Not automatically deleted | Represents the core, valid dataset; no expiry logic exists or was requested for this project's scope |
| rejected_events | Not automatically deleted | Kept for ongoing data quality analysis; a real deployment might purge entries older than a fixed window once they've been reviewed |

## Logs

| File | Retention | Rationale |
|---|---|---|
| logs/pipeline.log | Not automatically rotated or deleted | Grows indefinitely across all runs; a production deployment would need log rotation (e.g. via Python's RotatingFileHandler, or an external tool) - not implemented here, listed in future_improvements.md |

## Checkpoint

| Location | Retention | Rationale |
|---|---|---|
| checkpoint/ | Persists across restarts by design - this is what enables recovery | Should only be cleared deliberately (via python main.py clean) when a genuinely fresh start is wanted, since clearing it discards Spark's record of which files have already been processed |

## Manual Reset Procedure

For a genuinely clean local test environment:

1. python main.py clean - removes generated/archived CSVs and clears the checkpoint
2. In PostgreSQL: TRUNCATE TABLE events, rejected_events; - clears stored data (not automated; must be run manually, deliberately, since this is a destructive action)