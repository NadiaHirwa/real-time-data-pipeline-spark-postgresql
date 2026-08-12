# Data Contract

The enforceable rules every record must satisfy before it is considered valid. This is distinct from the [Data Dictionary](data_dictionary.md): the dictionary describes *what a column is*, this contract describes *what makes a value in that column acceptable*. A record failing any rule below is rejected — routed to `data/rejected/`, logged, and never written to PostgreSQL.

| Field | Rule | On Violation |
|---|---|---|
| `event_type` | MUST be exactly `view` or `purchase` (case-sensitive, after normalization to lowercase) | REJECT |
| `price` | MUST be >= 0 AND <= 10,000.00 | REJECT, tagged `invalid_or_negative_price` or `price_exceeds_maximum` |
| `quantity` | MUST be > 0 AND <= 100 | REJECT, tagged `invalid_or_zero_quantity` or `quantity_exceeds_maximum` |
| `user_id` | MUST NOT be null | REJECT |
| `product_id` | MUST NOT be null | REJECT |
| `event_timestamp` | MUST be a parseable timestamp, AND must not be more than a small tolerance (e.g. 5 minutes) in the future relative to processing time | REJECT |
| `event_id` | MUST match standard UUID format (8-4-4-4-12 hex), AND MUST be unique within a micro-batch | REJECT (tagged `invalid_event_id_format`) if malformed; duplicate dropped via `dropDuplicates()` if a repeat, not treated as a reject |
| `<entire row>` | MUST be a structurally valid CSV row (correct field count, parseable by Spark's CSV reader) | REJECT (tagged `malformed_csv_row`) |

## Normalization Applied Before Contract Checks

These are cleanup steps applied *before* the rules above are checked, so that trivial formatting differences don't cause unnecessary rejections:
- `event_type` is trimmed of whitespace and lowercased (`" Purchase "` → `purchase`) before being checked against the allowed values
- String fields are trimmed of leading/trailing whitespace

## What a Rejected Row Preserves

Normalization above is applied for *validation* purposes only — it never determines what gets recorded. A row failing any rule is written to `rejected_events` with its **original, pre-cast values intact**:

- A price of `not_a_number` is stored as `not_a_number`, not as the NULL that `try_cast()` produced while evaluating the rule. The same holds for `user_id`, `product_id`, `quantity`, and `event_timestamp`.
- A row tagged `malformed_csv_row` has no usable field values at all, so the raw CSV line Spark captured is stored in a dedicated `corrupt_record` column rather than being discarded.

This matters because a rejection record that shows only *which* rule failed, without the value that failed it, cannot be acted on — the reviewer cannot tell a generator bug from a genuine bad input. See [`data_dictionary.md`](data_dictionary.md) for the full `rejected_events` column list.

Note that `event_type` is stored in its normalized (lowercased, trimmed) form rather than the exact original text, so `"TELEPORT"` appears as `teleport`. The offending value is still fully visible; only its casing and surrounding whitespace are not recoverable.

## Explicitly Out of Contract Scope

- No validation is performed on `category` — it is optional and nullable by design
- No validation on the *semantic* plausibility of `product_id`/`user_id` values (e.g. we do not check that a `product_id` corresponds to a real product in some external catalog) — this pipeline has no product/user reference tables to validate against


