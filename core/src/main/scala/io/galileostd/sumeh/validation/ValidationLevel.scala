package io.galileostd.sumeh.validation

/**
 * Level at which a validation rule operates.
 *
 *   - ROW: the rule is evaluated per row and annotates bad rows in `_dq_errors`.
 *   - TABLE: the rule is an aggregation over the whole dataset (e.g. `has_mean`) and only runs in batch engines.
 */
sealed trait ValidationLevel

/** Concrete [[ValidationLevel]] values and a string parser. */
object ValidationLevel {

  /**
   * Per-row rule level — every row is checked independently and bad rows are tagged.
   */
  case object ROW extends ValidationLevel {
    override def toString = "ROW"
  }

  /**
   * Whole-dataset (aggregation) rule level — requires a batch engine.
   */
  case object TABLE extends ValidationLevel {
    override def toString = "TABLE"
  }

  /**
   * Parses a level from its string name.
   *
   * @param s
   *   The level name, `"ROW"` or `"TABLE"` (case-insensitive).
   * @return
   *   The matching [[ValidationLevel]].
   * @throws java.lang.IllegalArgumentException
   *   when the name is unknown.
   */
  def fromString(s: String): ValidationLevel = s.toUpperCase match {
    case "ROW"   => ROW
    case "TABLE" => TABLE
    case other   => throw new IllegalArgumentException(s"Unknown ValidationLevel: $other")
  }
}
