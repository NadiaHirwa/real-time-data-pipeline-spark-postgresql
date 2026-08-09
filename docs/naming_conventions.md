# Naming Conventions

## Generated Files

CSV files produced by data_generator.py follow this pattern:

```
events_YYYYMMDD_HHMMSS_ffffff.csv

Example: events_20260809_192424_303247.csv
```

- YYYYMMDD_HHMMSS: UTC timestamp at the moment the file was created
- ffffff: microseconds, included specifically to guarantee uniqueness even if two files were somehow created within the same second (the generator's 3-second interval makes this unlikely, but the format is deliberately collision-resistant)

## Python Code

- snake_case for all functions, variables, and module names (e.g. cast_and_normalize, write_valid_partition), following standard PEP 8 convention
- Module-level constants in UPPER_SNAKE_CASE (e.g. EVENT_SCHEMA, ALLOWED_EVENT_TYPES)
- Test functions named test_condition_expected_outcome (e.g. test_negative_price_is_rejected), so a failing test's name alone describes what broke

## Database Objects

- Table names: lowercase, plural, snake_case (events, rejected_events)
- Column names: lowercase snake_case, matching the field names used throughout the Python code exactly (event_id, user_id, event_timestamp) - deliberately avoiding any translation layer between "what Python calls it" and "what SQL calls it"
- Index names: idx_table_column (e.g. idx_events_timestamp)

## Rejection Reason Tags

Rejection reasons (the rejection_reason column, and the values used throughout spark_streaming.py's validation logic) follow a consistent pattern: field_or_condition_problem_type, all lowercase with underscores (e.g. missing_or_invalid_user_id, invalid_or_negative_price, future_timestamp). This makes them directly usable as GROUP BY values in SQL for data quality reporting without any string manipulation.

## Documentation Files

All docs live in docs/ as lowercase, underscore-separated .md files matching their content's subject directly (e.g. data_contract.md, performance_metrics.md) - no abbreviations or codes, so a filename alone is enough to guess its contents.