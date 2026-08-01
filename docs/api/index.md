# API Reference

The library is split into three artifacts. `core` is engine-agnostic; `spark` and `flink` depend on it and add their engine integrations.

| Artifact | Package prefix | What it contains |
|----------|----------------|------------------|
| `sumeh-core` | `io.galileostd.sumeh` | `RuleDefinition`, `RuleRegistry`, `RuleLoader` (JSON/CSV), `ValidationReport`, schema models |
| `sumeh-spark` | `io.galileostd.sumeh.spark` | `SparkValidator`, `SparkSchemaValidator`, `SparkProfiler`, `SparkRuleLoader` |
| `sumeh-flink` | `io.galileostd.sumeh.flink` | `FlinkValidator`, `FlinkRuleLoader` |

The scaladoc is generated from the source at site-build time (`sbt doc`) and staged under this directory:

- [Core API](core/index.html)
- [Spark API](spark/index.html)
- [Flink API](flink/index.html)

!!! note
    The generated HTML under `api/{core,spark,flink}/` is not committed to the repository — it is produced by the `docs` GitHub Actions workflow and regenerated on every docs build. To regenerate locally, see [Building & Contributing](../contributing.md).
