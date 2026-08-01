# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `SparkProfiler` — single-pass column profiling (completeness, cardinality, min/max/mean/std/sum) with optional sampling; `ProfileReport` with `tableStats`/`columnProfiles`/`toMap`/`toJson`.
- Rule `has_entropy` — Shannon entropy of a column value distribution (TABLE-level, batch-only).
- Rule `has_infogain` — normalized entropy `H / log2(cardinality)` (TABLE-level, batch-only).
- Rule `all_date_checks` — comprehensive per-row date validity suite (non-null + a real calendar date), on Spark and Flink.
- Rule-matrix coverage gate: statement coverage >= 90% for all modules, enforced in CI.
- Continuous integration: aggregated coverage report + Codecov upload.
- Documentation site (MkDocs Material) with scaladoc API reference — published to GitHub Pages.
