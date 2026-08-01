# Quickstart

Get from raw data to a `(good, bad)` split in a single pass. Sumeh runs the **same rule catalog** on **Apache Spark** (batch) and **Apache Flink** (streaming).

## 1. Installation

Artifacts are published to **GitHub Packages** (`https://maven.pkg.github.com/maltzsama/sumeh-dq`). Version numbers follow the release tags (`v0.1.0` → `0.1.0`).

```scala
resolvers += "GitHub Packages" at "https://maven.pkg.github.com/maltzsama/sumeh-dq"

libraryDependencies ++= Seq(
  "io.galileostd" %% "sumeh-core"  % "0.1.0",
  "io.galileostd" %% "sumeh-spark" % "0.1.0", // only if you validate Spark DataFrames
  "io.galileostd" %% "sumeh-flink" % "0.1.0"  // only if you validate Flink streams
)
```

Private repos (or any repo) require credentials to resolve:

```scala
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "<your-github-username>",
  "<personal-access-token>"
)
```

## 2. Define rules

`RuleDefinition.validated(field, checkType, value?, threshold?, execute?, metadata?)` validates the rule against `RuleRegistry` at build time and enriches `level`/`category`. An unknown check type throws immediately.

`field` is `Left("col")` for a single column, or `Right(List("a", "b"))` for multi-column rules (`are_complete`, `are_unique`, ...).

```scala
import io.galileostd.sumeh.rule.{ RuleDefinition, ListValue, LongValue, StringValue }

val rules = Seq(
  RuleDefinition.validated(Left("email"),  "is_complete"),
  RuleDefinition.validated(Left("age"),    "is_between",
    value = Some(ListValue(List(LongValue(18), LongValue(65))))),
  RuleDefinition.validated(Left("status"), "is_contained_in",
    value = Some(ListValue(List(StringValue("active"), StringValue("pending"))))),
  RuleDefinition.validated(Left("revenue"), "is_positive")
)
```

Or load rules from JSON, CSV, a Spark DataFrame, or a Flink Table — see [Rule Sources](#rule-sources).

## 3. Spark (batch)

```scala
import io.galileostd.sumeh.spark.SparkValidator

// One pass. Adds a `_dq_errors` column, returns a report.
val report = SparkValidator.validate(df, rules)

// Bifurcation — good vs. bad, no reprocessing.
val (good, bad) = report.split()
good.write.mode("overwrite").parquet("s3://bucket/good/")
bad .write.mode("overwrite").parquet("s3://bucket/bad/")

// Summary for dashboards / sinks / alerting.
val summary: Map[String, Any] = report.summary()
println(report.passRate) // fraction of evaluated rules that passed (skipped excluded)
```

`report.summary()` produces a `Map` you can drop straight into a sink or metrics endpoint:

```json
{
  "engine": "spark",
  "total_rows": 5,
  "passed": 2,
  "failed": 1,
  "errors": 0,
  "skipped": 1,
  "pass_rate": 0.667,
  "validations": [ { "check_type": "is_complete", "status": "PASS", "pass_rate": 1.0, "...": "..." } ]
}
```

## 4. Flink (streaming)

```scala
import io.galileostd.sumeh.rule.{ RuleDefinition, StringValue }
import io.galileostd.sumeh.flink.FlinkValidator

val rules = Seq(
  RuleDefinition.validated(Left("name"),   "is_complete"),
  RuleDefinition.validated(Left("amount"), "is_positive"),
  RuleDefinition.validated(Left("dt"),     "validate_date_format", value = Some(StringValue("yyyy-MM-dd")))
)

val validated = FlinkValidator.validate(stream, rules) // DataStream[Row]
val (good, bad) = validated.split()                     // side outputs, zero reprocessing
good.map(...).sinkTo(goodSink)
bad .map(...).sinkTo(badSink)
```

Streaming is **stateless by design**: each row is evaluated independently. Stateful/TABLE rules are reported as `SKIPPED` with a reason instead of failing the pipeline.

## 5. The validation report

A validation run produces a `ValidationReport`:

| Status | Meaning |
|--------|---------|
| `PASS` | The metric satisfied the rule expectation |
| `FAIL` | The metric violated the rule |
| `ERROR` | The rule could not be evaluated (unknown field, invalid config, runtime error) |
| `SKIPPED` | The rule was intentionally not executed — `execute=false`, wrong level, or engine doesn't support it (e.g. uniqueness in Flink streaming) — **with a reason** |

- `report.passed / failed / errors / skipped` — bucket results by status.
- `report.passRate` — passed ÷ evaluated (**skipped excluded**); `1.0` when nothing is evaluated.
- `report.summary(maxSampleIds = 100)` — flat JSON-friendly map, includes per-rule status + sampled violating row IDs.
- `report.split()` — the Bifurcation: `(good, bad)` via the engine's `Splittable`.

## Rule sources

### JSON

```json
[
  { "field": "email",  "check_type": "is_complete" },
  { "field": "age",    "check_type": "is_between",  "value": [18, 65] },
  { "field": "status", "check_type": "is_in",       "value": ["active", "pending"] }
]
```

```scala
import io.galileostd.sumeh.config.RuleLoader
val rules = RuleLoader.fromJsonString(json)   // List[RuleDefinition]
val back  = RuleLoader.toJson(rules)          // round-trip to JSON
```

### CSV

```csv
field,check_type,value,threshold,execute,level,category
email,is_complete,,1.0,true,ROW,completeness
age,is_between,"ListValue([LongValue(18),LongValue(65)])",1.0,true,ROW,comparison
```

`RuleLoader.fromCsvString(csv)` / `toCsv(rules)` — values use a lossless tagged format that round-trips through the `RuleValue` ADT.

### From a Spark DataFrame

```scala
import io.galileostd.sumeh.spark.config.SparkRuleLoader
val rules = SparkRuleLoader.fromDataFrame(rulesDf)                 // columns: field, check_type, ...
val rules = SparkRuleLoader.fromJsonColumn(rulesDf, "config")      // one JSON rule per row
```

### From a Flink Table

```scala
import io.galileostd.sumeh.flink.config.FlinkRuleLoader
val rules = FlinkRuleLoader.fromTable(rulesTable)
val rules = FlinkRuleLoader.fromJsonColumn(rulesTable, "config", tableEnv)
val rules = FlinkRuleLoader.fromTableResult(result, fieldNames)
```

## Next steps

- [Rule Catalog](rule-catalog.md) — every rule, its level, category, and engine support.
- [Spark](spark.md) — schema contracts and the data profiler.
- [Flink](flink.md) — streaming semantics and side-output bifurcation.
