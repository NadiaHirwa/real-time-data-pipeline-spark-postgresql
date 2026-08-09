# Data Contract

The enforceable rules every record must satisfy before it is considered valid. This is distinct from the [Data Dictionary](data_dictionary.md): the dictionary describes *what a column is*, this contract describes *what makes a value in that column acceptable*. A record failing any rule below is rejected — routed to `data/rejected/`, logged, and never written to PostgreSQL.

| Field | Rule | On Violation |
|---|---|---|
| `event_type` | MUST be exactly `view` or `purchase` (case-sensitive, after normalization to lowercase) | REJECT |
| `price` | MUST be >= 0 | REJECT |
| `quantity` | MUST be > 0 | REJECT |
| `user_id` | MUST NOT be null | REJECT |
| `product_id` | MUST NOT be null | REJECT |
| `event_timestamp` | MUST be a parseable timestamp, AND must not be more than a small tolerance (e.g. 5 minutes) in the future relative to processing time | REJECT |
| `event_id` | MUST be unique within a micro-batch | Duplicate dropped via `dropDuplicates()`; count logged, original-plus-duplicate not treated as two separate rejects |

## Normalization Applied Before Contract Checks

These are cleanup steps applied *before* the rules above are checked, so that trivial formatting differences don't cause unnecessary rejections:
- `event_type` is trimmed of whitespace and lowercased (`" Purchase "` → `purchase`) before being checked against the allowed values
- String fields are trimmed of leading/trailing whitespace

## Explicitly Out of Contract Scope

- No validation is performed on `category` — it is optional and nullable by design
- No validation on the *semantic* plausibility of `product_id`/`user_id` values (e.g. we do not check that a `product_id` corresponds to a real product in some external catalog) — this pipeline has no product/user reference tables to validate against
