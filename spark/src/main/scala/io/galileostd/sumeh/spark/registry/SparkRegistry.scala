package io.galileostd.sumeh.spark.registry

import io.galileostd.sumeh.rule.RuleRegistry
import io.galileostd.sumeh.spark.analyzer._
import io.galileostd.sumeh.spark.constraint._

/**
 * Internal wiring for the Spark engine: maps canonical `check_type` → `SparkConstraint`, and the rules that still need
 * a dedicated `SparkAnalyzer` → their analyzer.
 *
 * Internal component — not part of the public API. The only public entry point is
 * [[io.galileostd.sumeh.spark.SparkValidator.validate]], which is single-pass by construction. Most ROW rules have no
 * analyzer entry here: their metrics are computed by the shared aggregation inside `SparkValidator` from the rule's
 * `FailCondition`. Only rules that cannot share that aggregation (uniqueness, TABLE-level) appear in `analyzers`.
 */
private[sumeh] object SparkRegistry {

  /** Constraint per canonical `check_type` — every rule the Spark engine implements needs one. */
  private val constraints: Map[String, SparkConstraint] = Map(
    // Completeness
    "is_complete"  -> CompletenessConstraint,
    "are_complete" -> CompletenessConstraint,
    // Uniqueness
    "is_unique"  -> UniquenessConstraint,
    "are_unique" -> UniquenessConstraint,
    // Comparison
    "is_equal"                 -> GenericConstraint,
    "is_equal_than"            -> GenericConstraint,
    "is_between"               -> GenericConstraint,
    "is_greater_than"          -> GenericConstraint,
    "is_less_than"             -> GenericConstraint,
    "is_greater_or_equal_than" -> GenericConstraint,
    "is_less_or_equal_than"    -> GenericConstraint,
    "is_positive"              -> GenericConstraint,
    "is_negative"              -> GenericConstraint,
    "is_in_millions"           -> GenericConstraint,
    "is_in_billions"           -> GenericConstraint,
    // Membership
    "is_contained_in"  -> GenericConstraint,
    "not_contained_in" -> GenericConstraint,
    // Pattern
    "has_pattern" -> GenericConstraint,
    "is_legit"    -> GenericConstraint,
    // Date
    "is_today"             -> GenericConstraint,
    "is_t_minus_1"         -> GenericConstraint,
    "is_t_minus_2"         -> GenericConstraint,
    "is_t_minus_3"         -> GenericConstraint,
    "is_past_date"         -> GenericConstraint,
    "is_future_date"       -> GenericConstraint,
    "is_date_between"      -> GenericConstraint,
    "is_date_after"        -> GenericConstraint,
    "is_date_before"       -> GenericConstraint,
    "is_on_weekday"        -> GenericConstraint,
    "is_on_weekend"        -> GenericConstraint,
    "is_on_monday"         -> GenericConstraint,
    "is_on_tuesday"        -> GenericConstraint,
    "is_on_wednesday"      -> GenericConstraint,
    "is_on_thursday"       -> GenericConstraint,
    "is_on_friday"         -> GenericConstraint,
    "is_on_saturday"       -> GenericConstraint,
    "is_on_sunday"         -> GenericConstraint,
    "validate_date_format" -> GenericConstraint,
    "all_date_checks"      -> GenericConstraint,
    // SQL — batch only
    "satisfies" -> GenericConstraint,
    // Aggregation (TABLE)
    "has_min"         -> AggregationConstraint,
    "has_max"         -> AggregationConstraint,
    "has_sum"         -> AggregationConstraint,
    "has_mean"        -> AggregationConstraint,
    "has_std"         -> AggregationConstraint,
    "has_cardinality" -> AggregationConstraint,
    "has_entropy"     -> AggregationConstraint,
    "has_infogain"    -> AggregationConstraint,
    // Schema (TABLE)
    "validate_schema" -> SchemaConstraint
  )

  /** Analyzer per canonical `check_type` for rules that cannot share the validator's single aggregation. */
  private val analyzers: Map[String, SparkAnalyzer] = Map(
    "is_unique"       -> UniquenessAnalyzer,
    "are_unique"      -> MultiFieldUniquenessAnalyzer,
    "has_min"         -> AggregationAnalyzer,
    "has_max"         -> AggregationAnalyzer,
    "has_sum"         -> AggregationAnalyzer,
    "has_mean"        -> AggregationAnalyzer,
    "has_std"         -> AggregationAnalyzer,
    "has_cardinality" -> AggregationAnalyzer,
    "has_entropy"     -> AggregationAnalyzer,
    "has_infogain"    -> AggregationAnalyzer,
    "validate_schema" -> SchemaAnalyzer
  )

  /**
   * Returns the analyzer for a `check_type`, resolving aliases to their canonical name.
   *
   * Only rules that keep a dedicated analyzer (uniqueness, TABLE-level) have an entry; the validator calls this only for
   * those rules.
   *
   * @param checkType The rule type (may be an alias).
   * @return the `SparkAnalyzer` registered for the given check type
   * @throws java.lang.IllegalArgumentException if the `check_type` has no dedicated analyzer.
   */
  def getAnalyzer(checkType: String): SparkAnalyzer =
    analyzers.getOrElse(
      RuleRegistry.canonical(checkType),
      throw new IllegalArgumentException(
        s"'$checkType' has no dedicated analyzer in the Spark engine. " +
          "ROW-level metrics are computed by the shared single-pass aggregation in SparkValidator."
      )
    )

  /**
   * Returns the constraint for a `check_type`, resolving aliases to their canonical name.
   *
   * @param checkType The rule type (may be an alias).
   * @return the `SparkConstraint` registered for the given check type
   * @throws java.lang.IllegalArgumentException if the `check_type` is not implemented.
   */
  def getConstraint(checkType: String): SparkConstraint =
    constraints.getOrElse(
      RuleRegistry.canonical(checkType),
      throw new IllegalArgumentException(
        s"'$checkType' not implemented. Available: ${constraints.keys.mkString(", ")}"
      )
    )

  /**
   * All canonical `check_type`s implemented in the Spark engine, sorted.
   *
   * @return The sorted list of registered rule names.
   */
  def listImplemented(): List[String] = constraints.keys.toList.sorted
}
