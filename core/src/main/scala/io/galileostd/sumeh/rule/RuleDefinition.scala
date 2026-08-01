package io.galileostd.sumeh.rule

import java.time.{ LocalDate, LocalDateTime }
import scala.util.Try

import io.galileostd.sumeh.exception.SumehException

/**
 * A single data-quality rule.
 *
 * A `RuleDefinition` couples the declarative inputs (which field(s), which check, with which threshold/value) with
 * metadata that the engine needs to execute it (level, category). The raw `field`/`checkType`/`value` are exactly what
 * you'd write in JSON or CSV; `level` and `category` are normally filled in from the [[RuleRegistry]] by the
 * `validated` constructor so rules stay concise at the call site.
 *
 * Use [[RuleDefinition.validated]] to build one with registry validation, or [[RuleDefinition.fromMap]] to parse a
 * config map. `field` is `Left("col")` for single-column rules and `Right(List("a","b"))` for multi-column rules.
 *
 * Args: field: The column name(s) to validate — `Left` for one column, `Right` for several. checkType: The rule type
 * (e.g. `"is_complete"`); must exist in [[RuleRegistry]]. value: Threshold or comparison payload ([[RuleValue]]),
 * depending on the rule type. threshold: Pass-rate threshold in `[0.0, 1.0]`; the rule passes when the measured metric
 * meets it. execute: When `false` the rule is never run and always reported as `SKIPPED`. level: Validation level,
 * `ROW` or `TABLE` — normally auto-populated by the registry. category: Rule category (e.g. `"completeness"`) —
 * normally auto-populated by the registry. updatedAt: When the rule was last changed (parsed from `updated_at`).
 * metadata: Extra keys from the source config, preserved verbatim for round-tripping.
 */
final case class RuleDefinition(
    field: Either[String, List[String]],
    checkType: String,
    value: Option[RuleValue] = None,
    threshold: Double = 1.0,
    execute: Boolean = true,
    level: String = "ROW",
    category: String = "unknown",
    updatedAt: Option[LocalDateTime] = None,
    metadata: Map[String, Any] = Map.empty
) {

  /**
   * Flattened column name(s): a single name for `Left`, or a comma-joined string for `Right` (e.g. `"a,b"`).
   *
   * Used by CSV/JSON loaders and by `ValidationResult.fieldName` to render multi-column rules compactly.
   *
   * Returns: The column name, or comma-joined column names.
   */
  def fieldName: String = field.fold(identity, _.mkString(","))

  /**
   * Whether this rule operates at the given level.
   *
   * Both sides are uppercased and a `_LEVEL` suffix is stripped, so `"row"` and `"ROW_LEVEL"` both match the `ROW`
   * level. This drives the "no silent passes" behaviour: a `TABLE` rule is skipped, never silently run, on streaming
   * engines.
   *
   * Args: targetLevel: The level to test, e.g. `"ROW"` or `"TABLE"`.
   *
   * Returns: `true` when the rule's level equals `targetLevel`.
   */
  def isApplicableForLevel(targetLevel: String): Boolean = {
    val normalized = level.toUpperCase.replace("_LEVEL", "")
    val target     = targetLevel.toUpperCase.replace("_LEVEL", "")
    normalized == target
  }

  /**
   * Why this rule would be skipped at `targetLevel` on `engine`.
   *
   * The first applicable reason wins: `execute=false`, then a level mismatch, then engine support. When none apply the
   * rule can run and `None` is returned. Engines call this to decide whether to execute or skip with a reason.
   *
   * Args: targetLevel: The level the engine is running at (e.g. `"ROW"`). engine: The engine name (e.g. `"spark"`,
   * `"flink-streaming"`).
   *
   * Returns: A human-readable reason, or `None` if the rule can run.
   */
  def skipReason(targetLevel: String, engine: String): Option[String] =
    if (!execute) Some("execute=false")
    else if (!isApplicableForLevel(targetLevel))
      Some(s"Wrong level: expected '$targetLevel', got '$level'")
    else if (!RuleRegistry.isSupported(checkType, engine))
      Some(s"Engine '$engine' not supported for rule '$checkType'")
    else None

  /**
   * Compact human-readable rendering of the rule.
   *
   * Returns: A string like `RuleDef(field=email, check=is_complete, level=ROW, category=completeness)`.
   */
  override def toString: String = {
    val f    = field.fold(identity, cols => s"[${cols.mkString(",")}]")
    val meta = if (metadata.nonEmpty) s", +${metadata.size} meta" else ""
    s"RuleDef(field=$f, check=$checkType, level=$level, category=$category$meta)"
  }
}

/**
 * Companion with smart constructors and value/field parsing helpers.
 */
object RuleDefinition {

  /**
   * Smart constructor that validates the rule against [[RuleRegistry]].
   *
   * Enriches `level` and `category` from the registry manifest (mirroring Python's `__post_init__`), so callers only
   * need to specify `field`, `checkType`, and any rule-specific `value`/`threshold`. This is the primary way to build a
   * rule in code — it fails fast on a typo'd `checkType` instead of at validation time.
   *
   * Args: field: The column name(s) — `Left` for one, `Right` for several. checkType: The rule type; must exist in the
   * registry. value: Threshold or comparison value for the rule. threshold: Pass-rate threshold in `[0.0, 1.0]`.
   * execute: `false` to disable the rule. updatedAt: Rule update timestamp. metadata: Extra keys to preserve.
   *
   * Returns: A rule with `level`/`category` populated from the registry.
   *
   * Throws: [[io.galileostd.sumeh.exception.SumehException]] when `checkType` is not registered.
   */
  def validated( // ← era apply
      field: Either[String, List[String]],
      checkType: String,
      value: Option[RuleValue] = None,
      threshold: Double = 1.0,
      execute: Boolean = true,
      updatedAt: Option[LocalDateTime] = None,
      metadata: Map[String, Any] = Map.empty
  ): RuleDefinition = {
    val entry = RuleRegistry
      .getRule(checkType)
      .getOrElse(
        throw new SumehException(
          s"Invalid rule type '$checkType'. " +
            s"Available: ${RuleRegistry.listRules().take(10).mkString(", ")}..."
        )
      )

    new RuleDefinition(
      field = field,
      checkType = checkType,
      value = value,
      threshold = threshold,
      execute = execute,
      level = entry.level,
      category = entry.category,
      updatedAt = updatedAt,
      metadata = metadata
    )
  }

  /**
   * Creates a [[RuleDefinition]] from a raw config map.
   *
   * Parses the known keys (`field`, `check_type`, `value`, `threshold`, `execute`, `level`, `category`, `updated_at`)
   * and keeps every other key verbatim in `metadata`, so a source config survives a load→export round-trip. Parsing is
   * lenient: `value` goes through [[parseValue]], `threshold` falls back to `1.0`, `execute` accepts booleans and
   * truthy strings. The result is passed through [[validated]] for registry validation.
   *
   * Args: data: The rule as a key→value map (e.g. a CSV row or JSON object).
   *
   * Returns: The parsed rule.
   *
   * Throws: [[io.galileostd.sumeh.exception.SumehException]] when `check_type` is missing or unknown.
   */
  def fromMap(data: Map[String, Any]): RuleDefinition = {
    val knownFields = Set(
      "field",
      "check_type",
      "value",
      "threshold",
      "level",
      "category",
      "execute",
      "updated_at"
    )

    val field = parseField(data.getOrElse("field", ""))

    val value = data.get("value").flatMap(parseValue)

    val threshold = data.get("threshold").flatMap(v => Try(v.toString.toDouble).toOption).getOrElse(1.0)

    val execute = data
      .get("execute")
      .map {
        case b: Boolean => b
        case s: String =>
          val t = s.trim.toLowerCase
          t.isEmpty || Set("true", "1", "yes", "y", "t").contains(t)
        case _ => true
      }
      .getOrElse(true)

    val updatedAt = data.get("updated_at").flatMap(parseTimestamp)

    val metadata = data.filterNot { case (k, _) => knownFields.contains(k) }

    val checkType = data
      .get("check_type")
      .map(_.toString)
      .getOrElse(throw new SumehException("Missing required field: check_type"))

    RuleDefinition.validated(
      field = field,
      checkType = checkType,
      value = value,
      threshold = threshold,
      execute = execute,
      updatedAt = updatedAt,
      metadata = metadata
    )
  }

  /**
   * Parses a `field` config value into a single- or multi-column reference.
   *
   * Accepts a `List`, or a string that may be `[a,b]`, `a,b`, or a bare name. A single element becomes `Left(name)`;
   * two or more become `Right(List(...))`. Surrounding quotes are stripped.
   *
   * Args: input: The raw field value.
   *
   * Returns: `Left` for a single column, `Right` for multiple.
   */
  def parseField(input: Any): Either[String, List[String]] = input match {
    case list: List[_] =>
      val cols = list.map(_.toString.trim)
      if (cols.size > 1) Right(cols) else Left(cols.headOption.getOrElse(""))

    case s: String =>
      val trimmed = s.trim
        .stripPrefix("\"")
        .stripSuffix("\"")
        .stripPrefix("'")
        .stripSuffix("'")
        .trim

      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        val inner = trimmed.drop(1).dropRight(1).trim
        val cols =
          inner.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"").stripPrefix("'").stripSuffix("'")).toList
        if (cols.size > 1) Right(cols) else Left(cols.headOption.getOrElse(""))
      } else if (trimmed.contains(",")) {
        val cols = trimmed.split(",").map(_.trim).toList
        if (cols.size > 1) Right(cols) else Left(cols.headOption.getOrElse(""))
      } else {
        Left(trimmed)
      }

    case other => Left(other.toString.trim)
  }

  /**
   * Parses a raw `value` config value into a [[RuleValue]].
   *
   * Handles native JVM types (Boolean, Int/Long, Float/Double, LocalDate, LocalDateTime, List) and strings. Strings are
   * checked for the tagged CSV forms (`StringValue(...)`, `LongValue(...)`, ...) produced by
   * [[RuleValue.toTaggedString]], then for `[a,b]` list syntax, then coerced through date → date-time → long → double →
   * boolean → string, in that order. `null`, empty, and the literal `"NULL"` all become `None`.
   *
   * Args: input: The raw value from JSON/CSV/maps.
   *
   * Returns: The parsed value, or `None` when it represents a null.
   */
  def parseValue(input: Any): Option[RuleValue] = input match {
    case null                                              => None
    case s: String if s.toUpperCase == "NULL" || s.isEmpty => None
    case b: Boolean                                        => Some(BoolValue(b))
    case i: Int                                            => Some(LongValue(i.toLong))
    case l: Long                                           => Some(LongValue(l))
    case f: Float                                          => Some(DoubleValue(f.toDouble))
    case d: Double                                         => Some(DoubleValue(d))
    case d: LocalDate                                      => Some(DateValue(d))
    case dt: LocalDateTime                                 => Some(DateTimeValue(dt))
    case list: List[_]                                     => Some(ListValue(list.flatMap(v => parseValue(v))))
    case s: String if s.startsWith("StringValue(") && s.endsWith(")") =>
      Some(StringValue(s.stripPrefix("StringValue(").stripSuffix(")")))
    case s: String if s.startsWith("LongValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("LongValue(").stripSuffix(")")).collect { case l: LongValue => l }
    case s: String if s.startsWith("DoubleValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DoubleValue(").stripSuffix(")")).collect { case d: DoubleValue => d }
    case s: String if s.startsWith("BoolValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("BoolValue(").stripSuffix(")")).collect { case b: BoolValue => b }
    case s: String if s.startsWith("DateValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DateValue(").stripSuffix(")")).collect { case d: DateValue => d }
    case s: String if s.startsWith("DateTimeValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DateTimeValue(").stripSuffix(")")).collect { case dt: DateTimeValue => dt }
    case s: String if s.startsWith("ListValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("ListValue(").stripSuffix(")"))
    case s: String =>
      val trimmed = s.trim
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        val inner = trimmed.drop(1).dropRight(1)
        val items = inner.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).toList
        Some(ListValue(items.flatMap(v => parseValue(v))))
      } else {
        Try(LocalDate.parse(trimmed))
          .map(d => Some(DateValue(d)))
          .getOrElse(
            Try(LocalDateTime.parse(trimmed))
              .map(dt => Some(DateTimeValue(dt)))
              .getOrElse(
                Try(trimmed.toLong)
                  .map(l => Some(LongValue(l)))
                  .getOrElse(
                    Try(trimmed.toDouble)
                      .map(d => Some(DoubleValue(d)))
                      .getOrElse(
                        if (trimmed == "true" || trimmed == "false") Some(BoolValue(trimmed.toBoolean))
                        else Some(StringValue(trimmed))
                      )
                  )
              )
          )
      }
    case other => Some(StringValue(other.toString))
  }

  /**
   * Parses an `updated_at` value into a [[java.time.LocalDateTime]].
   *
   * Args: input: The raw value — a `LocalDateTime`, or a string parseable by `LocalDateTime.parse`.
   *
   * Returns: The parsed timestamp, or `None` when it can't be parsed.
   */
  private def parseTimestamp(input: Any): Option[LocalDateTime] = input match {
    case dt: LocalDateTime => Some(dt)
    case s: String         => Try(LocalDateTime.parse(s)).toOption
    case _                 => None
  }
}

/**
 * ADT for the `value` field of a rule.
 *
 * Typed values replace the `Any`-typed value of the Python port so that round-tripping is lossless: each variant knows
 * how to serialize itself (see [[toTaggedString]]) and be parsed back by [[RuleDefinition.parseValue]].
 */
sealed trait RuleValue {

  /**
   * Lossless export form used by [[io.galileostd.sumeh.config.RuleLoader.toCsv]].
   *
   * Wraps the value in its constructor name (e.g. `LongValue(42)`, `ListValue([LongValue(1),StringValue(a)])`) so it
   * survives CSV and round-trips through [[RuleDefinition.parseValue]] with no type ambiguity.
   *
   * Returns: The tagged string representation.
   */
  def toTaggedString: String
}

/** Companion with value conversion helpers. */
object RuleValue {

  /**
   * Converts a [[RuleValue]] to a plain JVM value.
   *
   * Dates and timestamps become `java.sql.Date`/`java.sql.Timestamp` so the result is directly usable with Spark's
   * `F.lit(...)`. Lists are converted recursively.
   *
   * Args: v: The rule value.
   *
   * Returns: A plain JVM value: `String`, `Long`, `Double`, `Boolean`, `java.sql.Date`/`java.sql.Timestamp`, or `List`.
   */
  def toAny(v: RuleValue): Any = v match {
    case StringValue(s)    => s
    case LongValue(l)      => l
    case DoubleValue(d)    => d
    case BoolValue(b)      => b
    case DateValue(d)      => java.sql.Date.valueOf(d)
    case DateTimeValue(dt) => java.sql.Timestamp.valueOf(dt)
    case ListValue(items)  => items.map(toAny)
  }
}

/**
 * String rule value.
 *
 * Holds the text payload for pattern, SQL, and format rules (e.g. a regex for `has_pattern`).
 */
final case class StringValue(v: String) extends RuleValue {

  /**
   * Serializes as `StringValue(<v>)`.
   *
   * Returns: The tagged string form.
   */
  def toTaggedString: String = s"StringValue($v)"
}

/**
 * Long rule value.
 *
 * Holds integer thresholds and comparison values (e.g. `LongValue(18)` in an `is_between` range).
 */
final case class LongValue(v: Long) extends RuleValue {

  /**
   * Serializes as `LongValue(<v>)`.
   *
   * Returns: The tagged string form.
   */
  def toTaggedString: String = s"LongValue($v)"
}

/**
 * Double rule value.
 *
 * Holds decimal thresholds and comparison values.
 */
final case class DoubleValue(v: Double) extends RuleValue {

  /**
   * Serializes as `DoubleValue(<v>)`.
   *
   * Returns: The tagged string form.
   */
  def toTaggedString: String = s"DoubleValue($v)"
}

/**
 * Boolean rule value.
 *
 * Holds `true`/`false` comparisons.
 */
final case class BoolValue(v: Boolean) extends RuleValue {

  /**
   * Serializes as `BoolValue(<v>)`.
   *
   * Returns: The tagged string form.
   */
  def toTaggedString: String = s"BoolValue($v)"
}

/**
 * Date rule value (a calendar date with no time component).
 */
final case class DateValue(v: LocalDate) extends RuleValue {

  /**
   * Serializes as `DateValue(<v>)`.
   *
   * Returns: The tagged string form.
   */
  def toTaggedString: String = s"DateValue($v)"
}

/**
 * Date-time rule value (a date with a time-of-day component).
 */
final case class DateTimeValue(v: LocalDateTime) extends RuleValue {

  /**
   * Serializes as `DateTimeValue(<v>)`.
   *
   * Returns: The tagged string form.
   */
  def toTaggedString: String = s"DateTimeValue($v)"
}

/**
 * List of rule values.
 *
 * Used by list-valued rules such as `is_between`, `is_contained_in`, and `not_contained_in`.
 */
final case class ListValue(v: List[RuleValue]) extends RuleValue {

  /**
   * Serializes as `ListValue([<v1>,<v2>,...])`, tagging each element recursively.
   *
   * Returns: The tagged string form.
   */
  def toTaggedString: String = s"ListValue([${v.map(_.toTaggedString).mkString(",")}])"
}
