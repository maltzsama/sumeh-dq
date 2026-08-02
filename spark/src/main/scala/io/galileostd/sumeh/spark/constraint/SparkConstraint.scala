package io.galileostd.sumeh.spark.constraint

import java.util.{ Locale, UUID }

import io.galileostd.sumeh.metric.MetricResult
import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.validation.{ ValidationLevel, ValidationResult, ValidationStatus }

// ============================================================================
// Base trait
// ============================================================================

/**
 * Compares a `MetricResult` against the rule's expectation and produces a `ValidationResult`.
 *
 * Constraints are the only component that knows about thresholds and pass/fail semantics; analyzers stay pure. Each
 * rule family has a constraint (completeness, uniqueness, generic, aggregation, schema).
 */
trait SparkConstraint {

  /**
   * Checks a metric against the rule.
   *
   * @param metric
   *   The computed metric.
   * @param rule
   *   The rule with its threshold/value.
   * @return
   *   The `ValidationResult`.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult

  /**
   * Formats a fraction in `[0.0, 1.0]` as a locale-independent percentage with two decimals.
   *
   * Uses `Locale.ROOT` so the message never depends on the JVM's default locale (which would render `1,00` on a pt-BR
   * host).
   *
   * @param fraction
   *   The fraction to format.
   * @return
   *   A string like `"98.00"`.
   */
  protected def pct(fraction: Double): String =
    String.format(Locale.ROOT, "%.2f", Double.box(fraction * 100))
}

// ============================================================================
// Completeness
// ============================================================================

/**
 * Constraint for completeness rules (`is_complete`, `are_complete`).
 *
 * Passes when the measured completeness pass rate meets `rule.threshold`.
 */
object CompletenessConstraint extends SparkConstraint {

  /**
   * Compares the completeness metric against the rule's threshold.
   *
   * @param metric
   *   A completeness metric (value = non-null fraction).
   * @param rule
   *   The rule carrying the threshold.
   * @return
   *   PASS when `metric.value >= rule.threshold`, otherwise FAIL with a percentage message.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.value >= rule.threshold
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.ROW,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      passRate = Some(metric.value),
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else
          Some(
            s"Completeness ${pct(metric.value)}% below threshold ${pct(rule.threshold)}%"
          ),
      expectedValue = Some(rule.threshold),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Uniqueness
// ============================================================================

/**
 * Constraint for uniqueness rules (`is_unique`, `are_unique`, `is_primary_key`, ...).
 *
 * Passes when the measured non-duplicate fraction meets `rule.threshold`.
 */
object UniquenessConstraint extends SparkConstraint {

  /**
   * Compares the uniqueness metric against the rule's threshold.
   *
   * @param metric
   *   A uniqueness metric (value = non-duplicate fraction).
   * @param rule
   *   The rule carrying the threshold.
   * @return
   *   PASS when `metric.value >= rule.threshold`, otherwise FAIL with a percentage message.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.value >= rule.threshold
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.ROW,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      passRate = Some(metric.value),
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else
          Some(
            s"Uniqueness ${pct(metric.value)}% below threshold ${pct(rule.threshold)}%"
          ),
      expectedValue = Some(rule.threshold),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Generic (comparison, membership, pattern, date)
// ============================================================================

/**
 * Constraint for comparison, membership, pattern, date, and SQL rules.
 *
 * Passes when the measured pass rate meets `rule.threshold`.
 */
object GenericConstraint extends SparkConstraint {

  /**
   * Compares a pass-rate metric against the rule's threshold.
   *
   * @param metric
   *   Any pass-rate metric (value in `[0.0, 1.0]`).
   * @param rule
   *   The rule carrying the threshold.
   * @return
   *   PASS when `metric.value >= rule.threshold`, otherwise FAIL with a percentage message.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.value >= rule.threshold
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.ROW,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      passRate = Some(metric.value),
      expectedValue = Some(rule.threshold),
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else
          Some(
            s"${rule.checkType} pass rate ${pct(metric.value)}% below threshold ${pct(rule.threshold)}%"
          ),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Aggregation (TABLE level)
// ============================================================================

/**
 * Constraint for TABLE-level aggregations.
 *
 * Compares the measured aggregation to the rule's expected `value`, which must be numeric. With `rule.tolerance == 0.0`
 * the comparison is exact. With `tolerance > 0`, a non-zero expected value accepts a relative error `|actual -
 * expected| / |expected| <= tolerance`; an expected value of `0` treats `tolerance` as an *absolute* bound on
 * `|actual|` (division by zero is impossible).
 *
 * A rule without a numeric `value` throws [[IllegalArgumentException]] — the caller turns that into an ERROR result, so
 * a misconfigured TABLE-level rule is reported instead of silently passing.
 */
object AggregationConstraint extends SparkConstraint {

  /**
   * Compares an aggregation metric against the rule's expected value.
   *
   * @param metric
   *   An aggregation metric (value = the aggregated number).
   * @param rule
   *   The rule carrying the expected `value` and relative `tolerance`.
   * @return
   *   PASS when the measured value matches the expectation within tolerance, otherwise FAIL.
   * @throws java.lang.IllegalArgumentException
   *   when `value` is missing or not numeric.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val expected = rule.value
      .collect {
        case io.galileostd.sumeh.rule.DoubleValue(d) => d
        case io.galileostd.sumeh.rule.LongValue(l)   => l.toDouble
      }
      .getOrElse(
        throw new IllegalArgumentException(
          s"${rule.checkType} requires a numeric value in 'value' (received: ${rule.value})"
        )
      )

    val passed =
      if (rule.tolerance <= 0.0) metric.value == expected
      else if (expected == 0.0) math.abs(metric.value) <= rule.tolerance
      else math.abs(metric.value - expected) / math.abs(expected) <= rule.tolerance

    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.TABLE,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      expectedValue = Some(expected),
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else Some(s"${rule.checkType}: expected $expected but got ${metric.value}"),
      metadata = metric.metadata
    )
  }
}

// ============================================================================
// Schema (TABLE level)
// ============================================================================

/**
 * Constraint for `validate_schema`.
 *
 * Passes when the schema metric's `passed` metadata flag is `true`.
 */
object SchemaConstraint extends SparkConstraint {

  /**
   * Compares the schema metric's `passed` flag.
   *
   * @param metric
   *   A schema metric (value `1.0`/`0.0`, `passed` flag in metadata).
   * @param rule
   *   The rule being checked.
   * @return
   *   PASS when the schema report passed, otherwise FAIL listing the type errors.
   */
  def check(metric: MetricResult, rule: RuleDefinition): ValidationResult = {
    val passed = metric.metadata.get("passed").exists(_.asInstanceOf[Boolean])
    ValidationResult(
      id = UUID.randomUUID().toString,
      checkType = rule.checkType,
      field = rule.field,
      level = ValidationLevel.TABLE,
      category = rule.category,
      status = if (passed) ValidationStatus.PASS else ValidationStatus.FAIL,
      // expectedValue is deliberately None: rule.value is a serialised JSON schema, not a numeric threshold.
      actualValue = Some(metric.value),
      message =
        if (passed) None
        else Some(s"validate_schema: schema does not match contract (${metric.metadata.get("type_errors")})"),
      metadata = metric.metadata
    )
  }
}
