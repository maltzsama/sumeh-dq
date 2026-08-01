package io.galileostd.sumeh.rule

final case class RuleEntry(
    checkType: String,
    level: String,
    category: String,
    description: String,
    engines: Set[String],
    aliasOf: Option[String] = None
)

object RuleRegistry {

  // ROW-level rules work in both batch and streaming
  private val row = Set("spark", "spark-streaming", "flink", "flink-streaming")

  // Row rules that require state/windowing (not available in either stateless streaming engine)
  private val streamingImpossible = Set("spark", "flink")

  // Uniqueness requires global state over the whole dataset — only Spark batch can do it
  private val uniqueness = Set("spark")

  // TABLE-level aggregations only work in batch (no streaming)
  private val batch = Set("spark", "flink")

  private val entries: List[RuleEntry] = List(
    // Completeness
    RuleEntry("is_complete", "ROW", "completeness", "Checks that field has no null values", row),
    RuleEntry("are_complete", "ROW", "completeness", "Checks that multiple fields have no null values", row),
    // Uniqueness
    RuleEntry("is_unique", "ROW", "uniqueness", "Checks that field values are unique", uniqueness),
    RuleEntry("are_unique", "ROW", "uniqueness", "Checks that combination of fields is unique", uniqueness),
    RuleEntry(
      "is_primary_key",
      "ROW",
      "uniqueness",
      "Alias for is_unique",
      uniqueness,
      aliasOf = Some("is_unique")
    ),
    RuleEntry(
      "is_composite_key",
      "ROW",
      "uniqueness",
      "Alias for are_unique",
      uniqueness,
      aliasOf = Some("are_unique")
    ),
    // Comparison
    RuleEntry("is_equal", "ROW", "comparison", "Checks if value equals specified threshold", row),
    RuleEntry("is_equal_than", "ROW", "comparison", "Checks if value equals another column", row),
    RuleEntry("is_between", "ROW", "comparison", "Checks if value is within range", row),
    RuleEntry("is_greater_than", "ROW", "comparison", "Checks if value > threshold", row),
    RuleEntry("is_less_than", "ROW", "comparison", "Checks if value < threshold", row),
    RuleEntry("is_greater_or_equal_than", "ROW", "comparison", "Checks if value >= threshold", row),
    RuleEntry("is_less_or_equal_than", "ROW", "comparison", "Checks if value <= threshold", row),
    RuleEntry("is_positive", "ROW", "comparison", "Checks if value > 0", row),
    RuleEntry("is_negative", "ROW", "comparison", "Checks if value < 0", row),
    RuleEntry("is_in_millions", "ROW", "comparison", "Checks if value >= 1,000,000", row),
    RuleEntry("is_in_billions", "ROW", "comparison", "Checks if value >= 1,000,000,000", row),
    // Membership
    RuleEntry("is_contained_in", "ROW", "membership", "Checks if value in allowed list", row),
    RuleEntry("not_contained_in", "ROW", "membership", "Checks if value not in disallowed list", row),
    RuleEntry("is_in", "ROW", "membership", "Alias for is_contained_in", row, aliasOf = Some("is_contained_in")),
    RuleEntry("not_in", "ROW", "membership", "Alias for not_contained_in", row, aliasOf = Some("not_contained_in")),
    // Pattern
    RuleEntry("has_pattern", "ROW", "pattern", "Checks if value matches regex pattern", row),
    RuleEntry("is_legit", "ROW", "pattern", "Checks if value is non-null and non-whitespace", row),
    // Date
    RuleEntry("is_today", "ROW", "date", "Checks if date equals today", row),
    RuleEntry("is_t_minus_1", "ROW", "date", "Checks if date equals yesterday", row),
    RuleEntry("is_t_minus_2", "ROW", "date", "Checks if date equals 2 days ago", row),
    RuleEntry("is_t_minus_3", "ROW", "date", "Checks if date equals 3 days ago", row),
    RuleEntry("is_yesterday", "ROW", "date", "Alias for is_t_minus_1", row, aliasOf = Some("is_t_minus_1")),
    RuleEntry("is_past_date", "ROW", "date", "Checks if date is before today", row),
    RuleEntry("is_future_date", "ROW", "date", "Checks if date is after today", row),
    RuleEntry("is_date_between", "ROW", "date", "Checks if date is within range", row),
    RuleEntry("is_date_after", "ROW", "date", "Checks if date is after specified date", row),
    RuleEntry("is_date_before", "ROW", "date", "Checks if date is before specified date", row),
    RuleEntry("is_on_weekday", "ROW", "date", "Checks if date falls on Monday-Friday", row),
    RuleEntry("is_on_weekend", "ROW", "date", "Checks if date falls on Saturday-Sunday", row),
    RuleEntry("is_on_monday", "ROW", "date", "Checks if date is Monday", row),
    RuleEntry("is_on_tuesday", "ROW", "date", "Checks if date is Tuesday", row),
    RuleEntry("is_on_wednesday", "ROW", "date", "Checks if date is Wednesday", row),
    RuleEntry("is_on_thursday", "ROW", "date", "Checks if date is Thursday", row),
    RuleEntry("is_on_friday", "ROW", "date", "Checks if date is Friday", row),
    RuleEntry("is_on_saturday", "ROW", "date", "Checks if date is Saturday", row),
    RuleEntry("is_on_sunday", "ROW", "date", "Checks if date is Sunday", row),
    RuleEntry("validate_date_format", "ROW", "date", "Checks if date string matches expected format", row),
    RuleEntry(
      "all_date_checks",
      "ROW",
      "date",
      "Runs comprehensive date validity suite (non-null and a real calendar date)",
      row
    ),
    // SQL
    RuleEntry("satisfies", "ROW", "sql", "Validates custom SQL condition", streamingImpossible),
    // Aggregation — batch only, no streaming
    RuleEntry("has_min", "TABLE", "aggregation", "Validates column minimum value", batch),
    RuleEntry("has_max", "TABLE", "aggregation", "Validates column maximum value", batch),
    RuleEntry("has_sum", "TABLE", "aggregation", "Validates column sum", batch),
    RuleEntry("has_mean", "TABLE", "aggregation", "Validates column average/mean", batch),
    RuleEntry("has_std", "TABLE", "aggregation", "Validates column standard deviation", batch),
    RuleEntry("has_cardinality", "TABLE", "aggregation", "Validates number of distinct values", batch),
    RuleEntry("has_entropy", "TABLE", "aggregation", "Validates column Shannon entropy", batch),
    RuleEntry(
      "has_infogain",
      "TABLE",
      "aggregation",
      "Validates column normalized entropy (H / log2(cardinality))",
      batch
    ),
    // Schema — batch only
    RuleEntry("validate_schema", "TABLE", "schema", "Validates DataFrame schema structure", batch)
  )

  private val manifest: Map[String, RuleEntry] =
    entries.map(e => e.checkType -> e).toMap

  def getRule(checkType: String): Option[RuleEntry] = manifest.get(checkType)
  def listRules(): List[String]                     = entries.map(_.checkType)
  def isSupported(checkType: String, engine: String): Boolean =
    manifest.get(checkType).exists(_.engines.contains(engine))
  def byCategory(category: String): List[RuleEntry] = entries.filter(_.category == category)
  def byLevel(level: String): List[RuleEntry]       = entries.filter(_.level == level.toUpperCase)
}
