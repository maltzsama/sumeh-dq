# Rule Catalog

Sumeh ships a single declarative catalog of rules that both engines share. Every rule is registered in `RuleRegistry` with a **level** (`ROW` = per-row check, `TABLE` = whole-dataset aggregation) and a **category**.

## Completeness

| Rule | Level | Description |
|------|-------|-------------|
| `is_complete` | ROW | Field has no null values |
| `are_complete` | ROW | Multiple fields have no null values |

## Uniqueness

!!! warning "Spark batch only"
    Uniqueness rules need global state over the whole dataset and are skipped (with a reason) in any streaming engine.

| Rule | Level | Description |
|------|-------|-------------|
| `is_unique` | ROW | Field values are unique |
| `are_unique` | ROW | Combination of fields is unique |
| `is_primary_key`¹ | ROW | Alias for `is_unique` |
| `is_composite_key`¹ | ROW | Alias for `are_unique` |

## Comparison

| Rule | Level | Description |
|------|-------|-------------|
| `is_equal` | ROW | Value equals the specified threshold |
| `is_equal_than` | ROW | Value equals another column |
| `is_between` | ROW | Value is within range |
| `is_greater_than` | ROW | Value > threshold |
| `is_less_than` | ROW | Value < threshold |
| `is_greater_or_equal_than` | ROW | Value >= threshold |
| `is_less_or_equal_than` | ROW | Value <= threshold |
| `is_positive` | ROW | Value > 0 |
| `is_negative` | ROW | Value < 0 |
| `is_in_millions` | ROW | Value >= 1,000,000 |
| `is_in_billions` | ROW | Value >= 1,000,000,000 |

## Membership

| Rule | Level | Description |
|------|-------|-------------|
| `is_contained_in` | ROW | Value is in the allowed list |
| `not_contained_in` | ROW | Value is not in the disallowed list |
| `is_in`¹ | ROW | Alias for `is_contained_in` |
| `not_in`¹ | ROW | Alias for `not_contained_in` |

## Pattern

| Rule | Level | Description |
|------|-------|-------------|
| `has_pattern` | ROW | Value matches a regex pattern |
| `is_legit` | ROW | Value is non-null and non-whitespace |

## Date

| Rule | Level | Description |
|------|-------|-------------|
| `is_today` | ROW | Date equals today |
| `is_t_minus_1` | ROW | Date equals yesterday |
| `is_t_minus_2` | ROW | Date equals 2 days ago |
| `is_t_minus_3` | ROW | Date equals 3 days ago |
| `is_yesterday`¹ | ROW | Alias for `is_t_minus_1` |
| `is_past_date` | ROW | Date is before today |
| `is_future_date` | ROW | Date is after today |
| `is_date_between` | ROW | Date is within a range |
| `is_date_after` | ROW | Date is after the specified date |
| `is_date_before` | ROW | Date is before the specified date |
| `is_on_weekday` | ROW | Date falls on Monday–Friday |
| `is_on_weekend` | ROW | Date falls on Saturday–Sunday |
| `is_on_monday` … `is_on_sunday` | ROW | Date falls on the named weekday |
| `validate_date_format` | ROW | Date string matches the expected format |
| `all_date_checks` | ROW | Runs the comprehensive date validity suite (non-null and a real calendar date) |

## SQL

| Rule | Level | Description |
|------|-------|-------------|
| `satisfies` | ROW | Validates a custom SQL condition |

`Satisfies` is skipped in streaming engines (`spark-streaming`, `flink-streaming`).

## Aggregation

!!! warning "Batch only"
    TABLE-level aggregations cannot run on an unbounded stream and are skipped (with a reason) in streaming engines.

| Rule | Level | Description |
|------|-------|-------------|
| `has_min` | TABLE | Column minimum value |
| `has_max` | TABLE | Column maximum value |
| `has_sum` | TABLE | Column sum |
| `has_mean` | TABLE | Column average/mean |
| `has_std` | TABLE | Column standard deviation |
| `has_cardinality` | TABLE | Number of distinct values |
| `has_entropy` | TABLE | Shannon entropy of the value distribution |
| `has_infogain` | TABLE | Normalized entropy of the value distribution |

### Entropy semantics

- **`has_entropy`** is the Shannon entropy `-Σ pᵢ·log₂(pᵢ)` of the value distribution.
- **`has_infogain`** is the normalized entropy `H / log₂(cardinality)` — `1.0` means a perfectly uniform distribution, `0.0` a single value.

Both are TABLE-level and compare against `value` within the relative `threshold`.

## Schema

| Rule | Level | Description |
|------|-------|-------------|
| `validate_schema` | TABLE | Validates the DataFrame schema structure |

## Aliases

Aliases resolve to their target at registration time (`is_in` → `is_contained_in`, `is_primary_key` → `is_unique`, `is_yesterday` → `is_t_minus_1`, ...). They behave exactly like the target rule.

## Introspection

`RuleRegistry` exposes the catalog at runtime:

```scala
import io.galileostd.sumeh.rule.RuleRegistry

RuleRegistry.listRules()                     // List[String]
RuleRegistry.byCategory("date")              // List[RuleEntry]
RuleRegistry.byLevel("TABLE")                // List[RuleEntry]
RuleRegistry.getRule("is_unique")            // Option[RuleEntry]
RuleRegistry.isSupported("is_unique", "flink-streaming") // false
```
