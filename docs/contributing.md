# Building & Contributing

## Requirements

- **JDK 17+**
- **sbt 1.11+**

## Project layout

```text
sumeh/
├── core/       # RuleDefinition, RuleRegistry, RuleLoader (JSON/CSV), ValidationReport,
│               # ValidationStatus, SchemaModels, MetricResult — no engine deps
├── spark/      # SparkValidator, SparkAnalyzer, SparkConstraint, SparkRuleLoader,
│               # SparkSchemaValidator, SparkProfiler, ValidatedSparkDataFrame
├── flink/      # FlinkValidator, DQProcessFunction, FlinkRuleLoader, ValidatedFlinkStream
├── project/    # sbt build
├── docs/       # MkDocs documentation site
└── build.sbt
```

`core` has **no runtime dependencies** beyond `upickle` and `slf4j-api` — it is pure data and logic.

## Build & test

```bash
# Full test suite (Scala 2.13, default Spark 4.x / Flink 2.x)
sbt test

# Cross-build tests
sbt -batch "++2.13.16" "test"
sbt -batch "++2.12.18" "core/test" "flink/test"

# Spark under Scala 2.12 (Spark 3.x line)
sbt -batch -Dspark.version=3.5.5 "++2.12.18" "spark/compile"

# Flink under the 1.x line
sbt -batch -Dflink.version=1.20.0 "flink/test"
```

Spark is cross-built as `2.12.18 + 2.13.16` against Spark 3.x, and `2.13.16` against Spark 4.x. Core and Flink are cross-built against both Scala versions. Flink is tested on both `2.2.0` (default) and `1.20.0`.

## Coverage

Statement coverage for all modules is gated at **>= 90%**, enforced in CI. The aggregate report is uploaded to Codecov.

```bash
sbt -batch "coverage" "test" "coverageAggregate" "coverageReport"
```

## Formatting

```bash
sbt scalafmtAll scalafmtCheckAll
```

## Continuous integration

`.github/workflows/ci.yml` runs the cross-build matrix on Java 17, enforces the coverage gate, and uploads the aggregated `cobertura.xml` to Codecov.

`.github/workflows/docs.yml` builds the documentation site and publishes it to the `gh-pages` branch (GitHub Pages). It runs on pushes to `main` and can be triggered manually via `workflow_dispatch`.

## Documentation site

The site is built with **MkDocs Material** (`mkdocs.yml`) from the `docs/` directory. The API Reference (scaladoc) is generated at build time:

```bash
sbt "core/Compile/doc" "spark/Compile/doc" "flink/Compile/doc"
mkdir -p docs/api
cp -r core/target/scala-2.13/api  docs/api/core
cp -r spark/target/scala-2.13/api docs/api/spark
cp -r flink/target/scala-2.13/api docs/api/flink

pip install -r requirements-docs.txt
mkdocs build --strict
mkdocs serve   # local preview at http://127.0.0.1:8000
```

The generated scaladoc under `docs/api/` is git-ignored — only `docs/api/index.md` is committed.

## Design principles

1. **No silent passes.** If a rule can't run, it is `SKIPPED` with a reason — never counted as a pass.
2. **Pure config layer.** `RuleLoader` only parses; you bring your own storage (S3, GCS, DBFS, JDBC, HTTP, ...).
3. **Streaming without surprises.** Flink validation is stateless per record; anything that needs state is flagged, not faked.
4. **Bifurcation first.** Bad rows are annotated in a single pass, so the split into `(good, bad)` is free.
