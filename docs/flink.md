# Flink (streaming)

The Flink engine validates `DataStream[Row]` records with side-output bifurcation. Streaming is **stateless by design**: each row is evaluated independently, so nothing is windowed, keyed, or buffered.

## Validate

```scala
import io.galileostd.sumeh.rule.{ RuleDefinition, StringValue }
import io.galileostd.sumeh.flink.FlinkValidator

val rules = Seq(
  RuleDefinition.validated(Left("name"),   "is_complete"),
  RuleDefinition.validated(Left("amount"), "is_positive"),
  RuleDefinition.validated(Left("dt"),     "validate_date_format", value = Some(StringValue("yyyy-MM-dd")))
)

val validated = FlinkValidator.validate(stream, rules) // ValidatedFlinkStream
```

## Bifurcation via side outputs

`split()` tags each record and routes it through Flink side outputs — good and bad records are separated with zero reprocessing.

```scala
val (good, bad) = validated.split()
good.map(...).sinkTo(goodSink)
bad .map(...).sinkTo(badSink)
```

Bad records carry a `_dq_errors` struct describing which rule failed and why.

## Streaming honesty

Rules that cannot run on an unbounded stream are reported as **`SKIPPED` with a reason** — never silently passed:

- **TABLE-level aggregations** (`has_mean`, `has_sum`, `has_cardinality`, `has_entropy`, `has_infogain`, `validate_schema`, ...) need the whole dataset and are skipped in streaming.
- **Uniqueness** rules (`is_unique`, `are_unique`, `is_primary_key`, `is_composite_key`) need global state and are skipped in streaming.
- **`satisfies`** (custom SQL) is skipped in streaming engines.

`report.summary()` / `report.passRate` count only evaluated rules — skipped rules are excluded from the pass rate, so the metric stays honest.

## Rule sources

```scala
import io.galileostd.sumeh.flink.config.FlinkRuleLoader
val rules = FlinkRuleLoader.fromTable(rulesTable)
val rules = FlinkRuleLoader.fromJsonColumn(rulesTable, "config", tableEnv)
val rules = FlinkRuleLoader.fromTableResult(result, fieldNames)
```

## Example: a full pipeline

```scala
import org.apache.flink.streaming.api.scala._
import io.galileostd.sumeh.rule.{ RuleDefinition, StringValue }
import io.galileostd.sumeh.flink.FlinkValidator

val env = StreamExecutionEnvironment.getExecutionEnvironment

val stream: DataStream[Row] = ...

val rules = Seq(
  RuleDefinition.validated(Left("order_id"), "is_complete"),
  RuleDefinition.validated(Left("amount"),   "is_positive"),
  RuleDefinition.validated(Left("event_dt"), "validate_date_format", value = Some(StringValue("yyyy-MM-dd")))
)

val (good, bad) = FlinkValidator.validate(stream, rules).split()

good.map(toParquet).sinkTo(goodSink)
bad .map(toQuarantine).sinkTo(badSink)

env.execute("sumeh-quality")
```
