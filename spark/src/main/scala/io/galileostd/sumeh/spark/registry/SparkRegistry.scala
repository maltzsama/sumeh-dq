package io.galileostd.sumeh.spark.registry

import io.galileostd.sumeh.rule.RuleRegistry
import io.galileostd.sumeh.spark.analyzer._
import io.galileostd.sumeh.spark.constraint._

/**
 * Maps `check_type` → `(SparkAnalyzer, SparkConstraint)` for every rule the Spark engine implements.
 *
 * The runtime analogue of the core `RuleRegistry` manifest: it wires each rule name to the analyzer that computes its
 * metric and the constraint that turns that metric into a pass/fail. Lookup canonicalizes aliases via
 * [[io.galileostd.sumeh.rule.RuleRegistry.canonical]], so the map holds only canonical names.
 */
object SparkRegistry {

  /** Wiring table: canonical `check_type` → `(analyzer, constraint)`. */
  private val registry: Map[String, (SparkAnalyzer, SparkConstraint)] = Map(
    // Completeness
    "is_complete"  -> (CompletenessAnalyzer, CompletenessConstraint),
    "are_complete" -> (MultiFieldCompletenessAnalyzer, CompletenessConstraint),
    // Uniqueness
    "is_unique"  -> (UniquenessAnalyzer, UniquenessConstraint),
    "are_unique" -> (MultiFieldUniquenessAnalyzer, UniquenessConstraint),
    // Comparison
    "is_equal"                 -> (ComparisonAnalyzer, GenericConstraint),
    "is_equal_than"            -> (ColumnComparisonAnalyzer, GenericConstraint),
    "is_between"               -> (BetweenAnalyzer, GenericConstraint),
    "is_greater_than"          -> (ComparisonAnalyzer, GenericConstraint),
    "is_less_than"             -> (ComparisonAnalyzer, GenericConstraint),
    "is_greater_or_equal_than" -> (ComparisonAnalyzer, GenericConstraint),
    "is_less_or_equal_than"    -> (ComparisonAnalyzer, GenericConstraint),
    "is_positive"              -> (ComparisonAnalyzer, GenericConstraint),
    "is_negative"              -> (ComparisonAnalyzer, GenericConstraint),
    "is_in_millions"           -> (ComparisonAnalyzer, GenericConstraint),
    "is_in_billions"           -> (ComparisonAnalyzer, GenericConstraint),
    // Membership
    "is_contained_in"  -> (MembershipAnalyzer, GenericConstraint),
    "not_contained_in" -> (MembershipAnalyzer, GenericConstraint),
    // Pattern
    "has_pattern" -> (PatternAnalyzer, GenericConstraint),
    "is_legit"    -> (LegitAnalyzer, GenericConstraint),
    // Date
    "is_today"        -> (DateAnalyzer, GenericConstraint),
    "is_t_minus_1"    -> (DateAnalyzer, GenericConstraint),
    "is_t_minus_2"    -> (DateAnalyzer, GenericConstraint),
    "is_t_minus_3"    -> (DateAnalyzer, GenericConstraint),
    "is_past_date"    -> (DateAnalyzer, GenericConstraint),
    "is_future_date"  -> (DateAnalyzer, GenericConstraint),
    "is_date_between" -> (DateBetweenAnalyzer, GenericConstraint),
    "is_date_after"   -> (DateComparisonAnalyzer, GenericConstraint),
    "is_date_before"  -> (DateComparisonAnalyzer, GenericConstraint),
    "is_on_weekday"   -> (DateAnalyzer, GenericConstraint),
    "is_on_weekend"   -> (DateAnalyzer, GenericConstraint),
    "is_on_monday"    -> (DateAnalyzer, GenericConstraint),
    "is_on_tuesday"   -> (DateAnalyzer, GenericConstraint),
    "is_on_wednesday" -> (DateAnalyzer, GenericConstraint),
    "is_on_thursday"  -> (DateAnalyzer, GenericConstraint),
    "is_on_friday"    -> (DateAnalyzer, GenericConstraint),
    "is_on_saturday"  -> (DateAnalyzer, GenericConstraint),
    "is_on_sunday"    -> (DateAnalyzer, GenericConstraint),
    // Aggregation (TABLE)
    "has_min"              -> (AggregationAnalyzer, AggregationConstraint),
    "has_max"              -> (AggregationAnalyzer, AggregationConstraint),
    "has_sum"              -> (AggregationAnalyzer, AggregationConstraint),
    "has_mean"             -> (AggregationAnalyzer, AggregationConstraint),
    "has_std"              -> (AggregationAnalyzer, AggregationConstraint),
    "has_cardinality"      -> (AggregationAnalyzer, AggregationConstraint),
    "has_entropy"          -> (AggregationAnalyzer, AggregationConstraint),
    "has_infogain"         -> (AggregationAnalyzer, AggregationConstraint),
    "validate_date_format" -> (DateFormatAnalyzer, GenericConstraint),
    "all_date_checks"      -> (DateAnalyzer, GenericConstraint),
    "validate_schema"      -> (SchemaAnalyzer, SchemaConstraint),
    "satisfies"            -> (SatisfiesAnalyzer, GenericConstraint)
  )

  /**
   * Returns the analyzer for a `check_type`, resolving aliases to their canonical name.
   *
   * @param checkType The rule type (may be an alias).
   * @return the `SparkAnalyzer` registered for the given check type
   * @throws java.lang.IllegalArgumentException if the `check_type` is not implemented in the Spark engine.
   */
  def getAnalyzer(checkType: String): SparkAnalyzer =
    registry
      .getOrElse(
        RuleRegistry.canonical(checkType),
        throw new IllegalArgumentException(
          s"'$checkType' not implemented in Spark engine. Available: ${registry.keys.mkString(", ")}"
        )
      )
      ._1

  /**
   * Returns the constraint for a `check_type`, resolving aliases to their canonical name.
   *
   * @param checkType The rule type (may be an alias).
   * @return the `SparkConstraint` registered for the given check type
   * @throws java.lang.IllegalArgumentException if the `check_type` is not implemented.
   */
  def getConstraint(checkType: String): SparkConstraint =
    registry
      .getOrElse(
        RuleRegistry.canonical(checkType),
        throw new IllegalArgumentException(
          s"'$checkType' not implemented. Available: ${registry.keys.mkString(", ")}"
        )
      )
      ._2

  /**
   * All canonical `check_type`s implemented in the Spark engine, sorted.
   *
   * @return The sorted list of registered rule names.
   */
  def listImplemented(): List[String] = registry.keys.toList.sorted
}
