# [0.1.0](https://github.com/maltzsama/sumeh-dq/compare/v0.0.0...v0.1.0) (2026-08-02)


### Bug Fixes

* **build:** keep publishLocal working when publish is skipped ([1320900](https://github.com/maltzsama/sumeh-dq/commit/1320900d2d5aa1016b5eb4f47e04c19077e4ffe8))
* **core,flink:** harden rule loading and stream validation ([1d6539b](https://github.com/maltzsama/sumeh-dq/commit/1d6539b491283550d7de7dca90b17537607398c7))
* **core:** format percentages with a fixed locale ([fe5e6aa](https://github.com/maltzsama/sumeh-dq/commit/fe5e6aa5d1ca289cda5a6738f7d04c7db313a9dc))
* **core:** generate timestamps in UTC ([5f94ee8](https://github.com/maltzsama/sumeh-dq/commit/5f94ee8cd7a71c5aa1240fe4598b5239a60073ab))
* **core:** reject CSV with unterminated quoted fields instead of silently splitting ([81fabf9](https://github.com/maltzsama/sumeh-dq/commit/81fabf9779633947904fc5870fcf45aca3f7d532))
* **core:** remove dead result-identity fields and unify result_id ([6fa4af7](https://github.com/maltzsama/sumeh-dq/commit/6fa4af7f4583309aaf3336da39498f6849c8072b))
* **core:** report duplicate_count as fail_count in summary ([c6e6272](https://github.com/maltzsama/sumeh-dq/commit/c6e6272fff3bd7b1456f3e9aa02edacc9e0ea445))
* **core:** restrict uniqueness rules to spark batch engine ([824a50f](https://github.com/maltzsama/sumeh-dq/commit/824a50f8c022df23e0c3f3d086364f10daf2d033))
* **flink:** key has_pattern cache by the regex, not checkType ([b468b12](https://github.com/maltzsama/sumeh-dq/commit/b468b129fb2ef98d356369b9ba0159b1d5297684))
* **flink:** match has_pattern as a partial search like Spark rlike ([42437bd](https://github.com/maltzsama/sumeh-dq/commit/42437bdd55c27589f5988b1d4ce37585c7581490))
* **flink:** reject rules targeting columns missing from the stream at construction ([4d2bb6d](https://github.com/maltzsama/sumeh-dq/commit/4d2bb6da660c3e806c4f977ae97c352c55f6d54a))
* **flink:** replace silent empty-string fallbacks with errors ([8620fe1](https://github.com/maltzsama/sumeh-dq/commit/8620fe1a8276b5a7122307aca1774f88815a5450))
* **flink:** skip has_pattern regex compilation for rules that will not run ([0bb3ec9](https://github.com/maltzsama/sumeh-dq/commit/0bb3ec9726bbaaa25c822a2eb9520fa065dba163))
* **flink:** support positional rows and declare output RowTypeInfo ([9bcfb6a](https://github.com/maltzsama/sumeh-dq/commit/9bcfb6a7d7593527a2135bcb16895f667b9918e2))
* **flink:** throw on malformed rule values instead of silent defaults ([9c25c08](https://github.com/maltzsama/sumeh-dq/commit/9c25c08039cf60c0a297f375ed299df4e6994958))
* **flink:** use getResolvedSchema and drop temporary views ([acc32c0](https://github.com/maltzsama/sumeh-dq/commit/acc32c0af431996cd7e1759baade9fd046cbc324))
* **flink:** validate rules once at job construction ([b303b10](https://github.com/maltzsama/sumeh-dq/commit/b303b10b41408ccf0d959371a4a2cf9c4f824044))
* **spark:** accept Spark native type names in schema contracts ([4ad30d5](https://github.com/maltzsama/sumeh-dq/commit/4ad30d5d7d25f6f9ae94235d9cd55c7aa123dd7e))
* **spark:** fill expectedValue in every constraint ([f76b782](https://github.com/maltzsama/sumeh-dq/commit/f76b782292c2044211d1a311b7f75eb177861525))
* **spark:** move streaming boilerplate message to metadata ([1a7a0c1](https://github.com/maltzsama/sumeh-dq/commit/1a7a0c19b3b563c1836a2ffd2e91bbc9b626efb5)), closes [passing-throu#threshold](https://github.com/passing-throu/issues/threshold)
* **spark:** read null aggregates with isNullAt instead of unboxing them ([969c3b2](https://github.com/maltzsama/sumeh-dq/commit/969c3b26afa27302a13234ea04be736dbb488691))
* **spark:** resolve rule fields case-insensitively, matching the session default ([de814cb](https://github.com/maltzsama/sumeh-dq/commit/de814cbe260f5022142c9e3cfc2fcf40b8df4537))
* **spark:** treat a null _dq_errors as good instead of dropping the row ([02cec1f](https://github.com/maltzsama/sumeh-dq/commit/02cec1f781869e7ee864aec2ab60e524d9583efe))


### Features

* **build:** publish engine-major-specific artifacts ([b073b82](https://github.com/maltzsama/sumeh-dq/commit/b073b8267953e13aae11ecee47357fde60297b8a))
* **ci:** add Maven Central publishing via Sonatype Central Portal ([64913ed](https://github.com/maltzsama/sumeh-dq/commit/64913edea853bf93a5f19e039df8e7c80023fa7e))
* consistent output contract across engines ([c642c26](https://github.com/maltzsama/sumeh-dq/commit/c642c266081897387e02602c2ddb5343cc90a2d5))
* **core:** add config loading infrastructure and schema models ([5e5acd6](https://github.com/maltzsama/sumeh-dq/commit/5e5acd6805d450020156397c230c8bb0a7012e15))
* **core:** default aggregation tolerance to floating-point precision ([e3e35ec](https://github.com/maltzsama/sumeh-dq/commit/e3e35ec035416e39e324b73314c57bd7576dde3f))
* **core:** separate aggregation tolerance from row-level threshold ([b963127](https://github.com/maltzsama/sumeh-dq/commit/b963127d81fda5bb75df5b11a687853a8461a775))
* **docs:** redesign documentation system with modern UI and GitHub Pages ([d650a38](https://github.com/maltzsama/sumeh-dq/commit/d650a38a536b065fee83045a0774a50201f80934))
* **flink:** implement streaming validation engine with side-output bifurcation ([e4c7389](https://github.com/maltzsama/sumeh-dq/commit/e4c738959be7dced0bc867d45c2448364aef4d8e))
* **jvm:** implement Spark engine with full 54-rule support ([da495b3](https://github.com/maltzsama/sumeh-dq/commit/da495b3a363a95126eeb63c43094f1458b06c7b3))
* **spark:** add new analyzers and complete schema validation ([52e5548](https://github.com/maltzsama/sumeh-dq/commit/52e5548788237a936d073d1746fa94587db4ed65))
* **spark:** align _dq_errors contract with the Python implementation ([f73e4ec](https://github.com/maltzsama/sumeh-dq/commit/f73e4ec336770791fcf1db40b7494d80125dd92c))
* **spark:** always annotate violating rows regardless of threshold ([434a116](https://github.com/maltzsama/sumeh-dq/commit/434a11658bf00fd134517c30ae2317d5288e0370))
* **spark:** native streaming validation via column expressions ([3d59a64](https://github.com/maltzsama/sumeh-dq/commit/3d59a64d77c1140d13d3bc5496fa81e0e8f08fe7))
* SparkProfiler, has_entropy/has_infogain/all_date_checks, README branding, Codecov ([40a8b70](https://github.com/maltzsama/sumeh-dq/commit/40a8b701cd8ed6f68ff2338bed3e8ba3f8a41a35))


### Performance Improvements

* **spark:** profile in a single aggregation pass ([d5af624](https://github.com/maltzsama/sumeh-dq/commit/d5af624acab8e72780d93a8e2be541016ec62402))
* **spark:** single-pass validation with a shared aggregation ([1c9727e](https://github.com/maltzsama/sumeh-dq/commit/1c9727e71d252efd59d7c3895e5f7d3ec645c978))

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
- Scaladoc API reference published to GitHub Pages (no separate docs framework).
