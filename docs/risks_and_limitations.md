# Risks and Limitations

Distinct from Future Improvements: this document lists what the system genuinely cannot do or has not been proven to do, as of this submission. Future Improvements lists what COULD be added later.

## Risks

| Risk | Mitigation in Place | Residual Risk |
|---|---|---|
| PostgreSQL becomes unreachable mid-batch | Files remain unarchived, retried on next trigger (see error_handling_and_recovery.md) | No exponential backoff; a persistently-down database would cause the same batch to be retried indefinitely at a fixed interval, generating continuous log noise without escalating |
| Disk fills up (data/incoming/, data/processed_archive/, checkpoint/ all grow over time) | Archiving keeps data/incoming/ small; no retention/deletion policy exists for data/processed_archive/ or logs/ | Long-running deployments would need a retention policy (see retention_policy.md) - not implemented, only documented as a gap |
| Large batch sizes | repartition(4) fix reduced connection overhead significantly (see performance_metrics.md) | Not tested beyond ~60 rows/batch; behavior at genuinely large batch sizes (thousands of rows) is unverified |
| Schema drift (generator's CSV columns change unexpectedly) | Explicit schema declaration means a genuinely missing/renamed column would produce nulls, which existing validation would catch as missing_or_invalid_* | Column REORDERING or a wholly different file format would not be handled gracefully |
| Windows file locks (observed directly during this project) | main.py clean now uses ignore_errors=True and reports failures instead of crashing | Underlying cause (OneDrive sync, possible lingering Java processes) not fully eliminated, only worked around defensively |

## Limitations

- **CSV-based simulation, not real streaming infrastructure.** This is an explicit, accepted constraint (see scope.md), not a limitation discovered after the fact - but it means findings here (throughput, latency) describe THIS specific architecture and would not directly transfer to a Kafka-based system.
- **Single-machine testing only.** No distributed Spark cluster or networked PostgreSQL instance was tested; latency and throughput numbers in performance_metrics.md reflect local-machine conditions only.
- **No automated tests for system-level failure scenarios** (database outage, mid-batch crash, deliberately corrupted CSV) - see test_cases.md's "System-Level Scenarios" section for the explicit list of what was and was not tested, and why.
- **No connection pooling.** Each partition opens and closes its own psycopg2 connection per batch. This was identified as the dominant cost in per-batch latency (see performance_metrics.md) and is listed as a concrete future improvement rather than fixed in this version.
- **Trigger interval tuning was not iterated on.** The configured 5-second interval was found to be too aggressive for the tested write pattern; a properly tuned value was not re-tested after this finding, due to time constraints (see performance_metrics.md).
- **No security hardening.** Database credentials are read from a local .env file (gitignored), but postgres_connection_details.txt (a required deliverable per the original brief) does store connection details as a checked-in file - see engineering_decisions.md's note on this explicit, documented trade-off.