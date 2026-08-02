package io.galileostd.sumeh.rule

/**
 * Metadata for a single rule in the catalog.
 *
 * Describes everything an engine needs to decide whether and how to run a rule: which level it operates at, which
 * category it belongs to, which engines can execute it, and whether it is an alias of another rule.
 *
 * @param checkType The rule name (e.g. `"is_complete"`).
 * @param level Validation level, `ROW` or `TABLE`.
 * @param category Rule category (completeness, uniqueness, comparison, ...).
 * @param description Human-readable description of what the rule checks.
 * @param engines Set of engine names where the rule is supported (e.g. `"spark"`, `"flink-streaming"`).
 * @param aliasOf When set, this rule is an alias of another `checkType` and behaves identically.
 */
final case class RuleEntry(
    checkType: String,
    level: String,
    category: String,
    description: String,
    engines: Set[String],
    aliasOf: Option[String] = None
)

/**
 * The rule catalog: a single source of truth for every supported `checkType`.
 *
 * Holds the level, category, description, and engine support for every rule. Both engines introspect this registry to
 * enforce the "no silent passes" contract — a rule the engine cannot run is skipped with a reason, never silently
 * accepted.
 */
object RuleRegistry {

  /**
   * Engines that can execute ROW-level rules — both batch and stateless streaming.
   */
  private val rowEngines = Set("spark", "spark-streaming", "flink", "flink-streaming")

  /**
   * Engines for TABLE-level aggregations, SQL, and schema rules — Spark batch only. There is no Flink batch engine in
   * this version; `flink-streaming` skips these rules with a reason.
   */
  private val batchEngines = Set("spark")

  /**
   * Engines for uniqueness rules, which require global state over the whole dataset — only Spark batch.
   */
  private val uniqueness = Set("spark")

  /**
   * The full catalog: one RuleEntry per supported rule, in declaration order.
   */
  private val entries: List[RuleEntry] = List(
    // Completeness
    RuleEntry("is_complete", "ROW", "completeness", "Checks that field has no null values", rowEngines),
    RuleEntry("are_complete", "ROW", "completeness", "Checks that multiple fields have no null values", rowEngines),
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
    RuleEntry("is_equal", "ROW", "comparison", "Checks if value equals specified threshold", rowEngines),
    RuleEntry("is_equal_than", "ROW", "comparison", "Checks if value equals another column", rowEngines),
    RuleEntry("is_between", "ROW", "comparison", "Checks if value is within range", rowEngines),
    RuleEntry("is_greater_than", "ROW", "comparison", "Checks if value > threshold", rowEngines),
    RuleEntry("is_less_than", "ROW", "comparison", "Checks if value < threshold", rowEngines),
    RuleEntry("is_greater_or_equal_than", "ROW", "comparison", "Checks if value >= threshold", rowEngines),
    RuleEntry("is_less_or_equal_than", "ROW", "comparison", "Checks if value <= threshold", rowEngines),
    RuleEntry("is_positive", "ROW", "comparison", "Checks if value > 0", rowEngines),
    RuleEntry("is_negative", "ROW", "comparison", "Checks if value < 0", rowEngines),
    RuleEntry("is_in_millions", "ROW", "comparison", "Checks if value >= 1,000,000", rowEngines),
    RuleEntry("is_in_billions", "ROW", "comparison", "Checks if value >= 1,000,000,000", rowEngines),
    // Membership
    RuleEntry("is_contained_in", "ROW", "membership", "Checks if value in allowed list", rowEngines),
    RuleEntry("not_contained_in", "ROW", "membership", "Checks if value not in disallowed list", rowEngines),
    RuleEntry(
      "is_in",
      "ROW",
      "membership",
      "Alias for is_contained_in",
      rowEngines,
      aliasOf = Some("is_contained_in")
    ),
    RuleEntry(
      "not_in",
      "ROW",
      "membership",
      "Alias for not_contained_in",
      rowEngines,
      aliasOf = Some("not_contained_in")
    ),
    // Pattern
    RuleEntry("has_pattern", "ROW", "pattern", "Checks if value matches regex pattern", rowEngines),
    RuleEntry("is_legit", "ROW", "pattern", "Checks if value is non-null and non-whitespace", rowEngines),
    // Date
    RuleEntry("is_today", "ROW", "date", "Checks if date equals today", rowEngines),
    RuleEntry("is_t_minus_1", "ROW", "date", "Checks if date equals yesterday", rowEngines),
    RuleEntry("is_t_minus_2", "ROW", "date", "Checks if date equals 2 days ago", rowEngines),
    RuleEntry("is_t_minus_3", "ROW", "date", "Checks if date equals 3 days ago", rowEngines),
    RuleEntry(
      "is_yesterday",
      "ROW",
      "date",
      "Alias for is_t_minus_1",
      rowEngines,
      aliasOf = Some("is_t_minus_1")
    ),
    RuleEntry("is_past_date", "ROW", "date", "Checks if date is before today", rowEngines),
    RuleEntry("is_future_date", "ROW", "date", "Checks if date is after today", rowEngines),
    RuleEntry("is_date_between", "ROW", "date", "Checks if date is within range", rowEngines),
    RuleEntry("is_date_after", "ROW", "date", "Checks if date is after specified date", rowEngines),
    RuleEntry("is_date_before", "ROW", "date", "Checks if date is before specified date", rowEngines),
    RuleEntry("is_on_weekday", "ROW", "date", "Checks if date falls on Monday-Friday", rowEngines),
    RuleEntry("is_on_weekend", "ROW", "date", "Checks if date falls on Saturday-Sunday", rowEngines),
    RuleEntry("is_on_monday", "ROW", "date", "Checks if date is Monday", rowEngines),
    RuleEntry("is_on_tuesday", "ROW", "date", "Checks if date is Tuesday", rowEngines),
    RuleEntry("is_on_wednesday", "ROW", "date", "Checks if date is Wednesday", rowEngines),
    RuleEntry("is_on_thursday", "ROW", "date", "Checks if date is Thursday", rowEngines),
    RuleEntry("is_on_friday", "ROW", "date", "Checks if date is Friday", rowEngines),
    RuleEntry("is_on_saturday", "ROW", "date", "Checks if date is Saturday", rowEngines),
    RuleEntry("is_on_sunday", "ROW", "date", "Checks if date is Sunday", rowEngines),
    RuleEntry("validate_date_format", "ROW", "date", "Checks if date string matches expected format", rowEngines),
    RuleEntry(
      "all_date_checks",
      "ROW",
      "date",
      "Runs comprehensive date validity suite (non-null and a real calendar date)",
      rowEngines
    ),
    // SQL — batch only
    RuleEntry("satisfies", "ROW", "sql", "Validates custom SQL condition", batchEngines),
    // Aggregation — batch only, no streaming
    RuleEntry("has_min", "TABLE", "aggregation", "Validates column minimum value", batchEngines),
    RuleEntry("has_max", "TABLE", "aggregation", "Validates column maximum value", batchEngines),
    RuleEntry("has_sum", "TABLE", "aggregation", "Validates column sum", batchEngines),
    RuleEntry("has_mean", "TABLE", "aggregation", "Validates column average/mean", batchEngines),
    RuleEntry("has_std", "TABLE", "aggregation", "Validates column standard deviation", batchEngines),
    RuleEntry("has_cardinality", "TABLE", "aggregation", "Validates number of distinct values", batchEngines),
    RuleEntry("has_entropy", "TABLE", "aggregation", "Validates column Shannon entropy", batchEngines),
    RuleEntry(
      "has_infogain",
      "TABLE",
      "aggregation",
      "Validates column normalized entropy (H / log2(cardinality))",
      batchEngines
    ),
    // Schema — batch only
    RuleEntry("validate_schema", "TABLE", "schema", "Validates DataFrame schema structure", batchEngines)
  )

  /** Lookup index: `checkType` → RuleEntry. */
  private val manifest: Map[String, RuleEntry] =
    entries.map(e => e.checkType -> e).toMap

  /**
   * Looks up a rule's metadata by `checkType`.
   *
   * @param checkType The rule name.
   * @return The rule's [[RuleEntry]], or `None` if it is not registered.
   */
  def getRule(checkType: String): Option[RuleEntry] = manifest.get(checkType)

  /**
   * All registered rule names, in declaration order.
   *
   * @return The full list of `checkType` names (aliases included).
   */
  def listRules(): List[String] = entries.map(_.checkType)

  /**
   * Whether an engine can execute the given rule.
   *
   * Used together with [[io.galileostd.sumeh.rule.RuleDefinition.skipReason]] so unsupported rules are skipped with a
   * reason instead of silently passing.
   *
   * @param checkType The rule name.
   * @param engine The engine name (e.g. `"spark"`, `"flink-streaming"`).
   * @return `true` when the engine is in the rule's `engines` set.
   */
  def isSupported(checkType: String, engine: String): Boolean =
    manifest.get(checkType).exists(_.engines.contains(engine))

  /**
   * Resolves an alias to its canonical `checkType`, leaving non-alias rules unchanged.
   *
   * Engines dispatch on the canonical name while still reporting the original `checkType` to the user, so an alias is
   * never missed by one engine and implemented by another.
   *
   * @param checkType the canonical rule name after resolving any alias
   * @return The canonical `checkType`, or the input when it is not an alias.
   */
  def canonical(checkType: String): String =
    manifest.get(checkType).flatMap(_.aliasOf).getOrElse(checkType)

  /**
   * Rules belonging to a category.
   *
   * @param category The category name (e.g. `"date"`, `"aggregation"`, case-insensitive).
   * @return The matching [[RuleEntry]]s in declaration order.
   */
  def byCategory(category: String): List[RuleEntry] =
    entries.filter(_.category == category.toLowerCase)

  /**
   * Rules at a given level.
   *
   * @param level The level name — `ROW` or `TABLE` (case-insensitive).
   * @return The matching [[RuleEntry]]s in declaration order.
   */
  def byLevel(level: String): List[RuleEntry] = entries.filter(_.level == level.toUpperCase)
}
