# Spark (batch)

The Spark engine validates `DataFrame`s with **column-vectorized** execution — row data is never collected to the driver. Every row rule annotates a `_dq_errors` column in a single pass.

## Validate

```scala
import io.galileostd.sumeh.rule.{ RuleDefinition, LongValue, ListValue }
import io.galileostd.sumeh.spark.SparkValidator

val rules = Seq(
  RuleDefinition.validated(Left("email"),  "is_complete"),
  RuleDefinition.validated(Left("age"),    "is_between", value = Some(ListValue(List(LongValue(18), LongValue(65))))),
  RuleDefinition.validated(Left("revenue"), "is_positive")
)

val report = SparkValidator.validate(df, rules)   // ValidationReport[DataFrame]
```

## Bifurcation

`report.split()` returns `(good, bad)` with **zero extra scans** — bad rows were already tagged in the validation pass.

```scala
val (good, bad) = report.split()
good.write.mode("overwrite").parquet("s3://bucket/good/")
bad .write.mode("overwrite").parquet("s3://bucket/bad/")
```

Bad rows carry a `_dq_errors` struct describing which rule failed and why.

## The report

```scala
report.passed / report.failed / report.errors / report.skipped  // buckets by status
report.passRate                                                // passed ÷ evaluated (skipped excluded)
report.summary(maxSampleIds = 100)                             // flat JSON-friendly Map
report.split()                                                 // (good, bad)
```

## Rule sources

```scala
import io.galileostd.sumeh.spark.config.SparkRuleLoader
val rules = SparkRuleLoader.fromDataFrame(rulesDf)            // columns: field, check_type, ...
val rules = SparkRuleLoader.fromJsonColumn(rulesDf, "config") // one JSON rule per row
```

## Schema contracts

Beyond value rules, validate the *shape* of your data — types, nullability, comments, and nested struct/array schemas.

```scala
import io.galileostd.sumeh.schema.{ SchemaDef, ColumnDef }
import io.galileostd.sumeh.spark.schema.SparkSchemaValidator

val contract = SchemaDef(
  columns = List(
    ColumnDef("id", "integer", nullable = false, requireComment = true),
    ColumnDef("name", "string"),
    ColumnDef("tags", "array", elementType = Some("string")),
    ColumnDef("address", "struct", fields = Some(List(
      ColumnDef("street", "string"),
      ColumnDef("zip", "integer")
    )))
  ),
  strictColumns = true  // extra columns in the data become errors
)

val report = SparkSchemaValidator.validate(df, contract)
report.passed         // Boolean
report.typeErrors     // col -> "Expected X, got Y"
report.missingCols    // required columns that are absent
report.metadataErrors // comment / nullability violations
report.extraCols      // only when strictColumns = true
```

Or declare the contract in a `Map`:

```scala
val contract = SchemaDef.fromMap(Map(
  "id"     -> Map("type" -> "integer", "nullable" -> false, "require_comment" -> true),
  "name"   -> "string",
  "tags"   -> Map("type" -> "array", "element_type" -> "string"),
  "address" -> Map(
    "type"   -> "struct",
    "fields" -> Map("street" -> "string", "zip" -> "integer")
  )
), strict = true)
```

Run it as a rule too: `RuleDefinition.validated(Left("_schema"), "validate_schema")`.

## Data profiling

Get column-level statistics without writing any validation rules — a single pass, reusing the same analyzers as validation.

```scala
import io.galileostd.sumeh.spark.SparkProfiler

val profile = SparkProfiler.profile(df)                            // ProfileReport
val profile = SparkProfiler.profile(df, sampleFraction = Some(0.1)) // sampled

profile.columnProfiles("revenue").mean        // Option[Double]
profile.columnProfiles("email").distinctCount
profile.tableStats                            // Map(total_rows, columns_count, execution_time_ms)
profile.toMap                                  // Map[String, Any]
profile.toJson                                 // JSON payload for dashboards / metrics
```

For every column it measures completeness and cardinality; numeric columns additionally get `min`, `max`, `mean`, `std_dev`, and `sum`. Output mirrors the Python `profile()` structure: `{ "table_stats": {...}, "column_profiles": { col -> {...} } }`.
