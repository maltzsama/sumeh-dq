package io.galileostd.sumeh.validation

/**
 * Level at which a validation rule operates.
 *
 *   - ROW: the rule is evaluated per row and annotates bad rows in `_dq_errors`.
 *   - TABLE: the rule is an aggregation over the whole dataset (e.g. has_mean) and only runs in batch engines.
 */
sealed trait ValidationLevel

/** Concrete ValidationLevel values and a string parser. */
object ValidationLevel {

  /** Per-row rule level. */
  case object ROW extends ValidationLevel {
    override def toString = "ROW"
  }

  /** Whole-dataset (aggregation) rule level. */
  case object TABLE extends ValidationLevel {
    override def toString = "TABLE"
  }

  /**
   * Parses a level from its string name ("ROW" / "TABLE").
   *
   * Args: s: The level name.
   *
   * Returns: The matching ValidationLevel.
   *
   * Throws: IllegalArgumentException if the name is unknown.
   */
  def fromString(s: String): ValidationLevel = s.toUpperCase match {
    case "ROW"   => ROW
    case "TABLE" => TABLE
    case other   => throw new IllegalArgumentException(s"Unknown ValidationLevel: $other")
  }
}
