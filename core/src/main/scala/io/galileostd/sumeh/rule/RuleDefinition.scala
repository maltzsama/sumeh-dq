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
 * @param field The column name(s) to validate — `Left` for one column, `Right` for several.
 * @param checkType The rule type (e.g. `"is_complete"`); must exist in [[RuleRegistry]].
 * @param value Threshold or comparison payload ([[RuleValue]]), depending on the rule type.
 *
 * @param threshold minimum fraction of rows that must pass, for ROW-level rules. Ignored for TABLE-level rules.
 * @param tolerance max relative error for TABLE rules; default `1e-9`; absolute when expected value is `0.0`
 *
 * @param execute When `false` the rule is never run and always reported as `SKIPPED`.
 * @param level Validation level, `ROW` or `TABLE` — normally auto-populated by the registry.
 * @param category Rule category (e.g. `"completeness"`) — normally auto-populated by the registry.
 * @param updatedAt When the rule was last changed (parsed from `updated_at`).
 * @param metadata Extra keys from the source config, preserved verbatim for round-tripping.
 */
final case class RuleDefinition(
    field: Either[String, List[String]],
    checkType: String,
    value: Option[RuleValue] = None,
    threshold: Double = 1.0,
    tolerance: Double = 1e-9,
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
   * @return the field name — a single column or comma-joined columns
   */
  def fieldName: String = field.fold(identity, _.mkString(","))

  /**
   * Whether this rule operates at the given level.
   *
   * Both sides are uppercased and a `_LEVEL` suffix is stripped, so `"row"` and `"ROW_LEVEL"` both match the `ROW`
   * level. This drives the "no silent passes" behaviour: a `TABLE` rule is skipped, never silently run, on streaming
   * engines.
   *
   * @param targetLevel The level to test, e.g. `"ROW"` or `"TABLE"`.
   * @return whether the rule targets this validation level
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
   * @param targetLevel The level the engine is running at (e.g. `"ROW"`).
   * @param engine The engine name (e.g. `"spark"`, `"flink-streaming"`).
   * @return why the rule is inapplicable; `None` if it can run
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
   * @return a compact human-readable summary: field, check, level, category
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
   * @param field `Left` for single-column, `Right` for multi-column rules
   * @param checkType The rule type; must exist in the registry.
   * @param value Threshold or comparison value for the rule.
   * @param threshold Pass-rate threshold in `[0.0, 1.0]`.
   * @param tolerance Relative tolerance for TABLE-level aggregation rules; default `1e-9`, `0.0` for exact match.
   * @param execute `false` to disable the rule.
   * @param level Optional override for the registry default (e.g. when loading from config)
   * @param category Optional user-supplied category overriding the registry default.
   * @param updatedAt Rule update timestamp.
   * @param metadata Extra keys to preserve.
   * @return A rule with `level`/`category` populated from the registry (or the supplied overrides).
   * @throws io.galileostd.sumeh.exception.SumehException when `checkType` is not registered.
   */
  def validated(
      field: Either[String, List[String]],
      checkType: String,
      value: Option[RuleValue] = None,
      threshold: Double = 1.0,
      tolerance: Double = 1e-9,
      execute: Boolean = true,
      level: Option[String] = None,
      category: Option[String] = None,
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
      tolerance = tolerance,
      execute = execute,
      level = level.getOrElse(entry.level),
      category = category.getOrElse(entry.category),
      updatedAt = updatedAt,
      metadata = metadata
    )
  }

  /**
   * Creates a [[RuleDefinition]] from a raw config map.
   *
   * Parses the known keys (`field`, `check_type`, `value`, `threshold`, `tolerance`, `execute`, `level`, `category`,
   * `updated_at`) and keeps every other key verbatim in `metadata`, so a source config survives a load→export
   * round-trip. Parsing is lenient: `value` goes through [[parseValue]], `threshold` falls back to `1.0`, `tolerance`
   * falls back to `1e-9`, `execute` accepts booleans and truthy strings. The result is passed through [[validated]] for
   * registry validation.
   *
   * @param data The rule as a key→value map (e.g. a CSV row or JSON object).
   * @return a [[RuleDefinition]] whose `level` and `category` are filled from the registry
   * @throws io.galileostd.sumeh.exception.SumehException when `check_type` is missing or unknown.
   */
  def fromMap(data: Map[String, Any]): RuleDefinition = {
    val knownFields = Set(
      "field",
      "check_type",
      "value",
      "threshold",
      "tolerance",
      "level",
      "category",
      "execute",
      "updated_at"
    )

    val field = parseField(data.getOrElse("field", ""))

    val value = data.get("value").flatMap(parseValue)

    val threshold = data.get("threshold").flatMap(v => Try(v.toString.toDouble).toOption).getOrElse(1.0)

    val tolerance = data.get("tolerance").flatMap(v => Try(v.toString.toDouble).toOption).getOrElse(1e-9)

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

    val level = data
      .get("level")
      .map(_.toString)
      .map(_.trim.toUpperCase.replace("_LEVEL", ""))
      .filter(_.nonEmpty)

    val category = data.get("category").map(_.toString).map(_.trim).filter(_.nonEmpty)

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
      tolerance = tolerance,
      execute = execute,
      level = level,
      category = category,
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
   * @param input The raw field value.
   * @return `Left` wrapping a single column name, `Right` holding multiple
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
          splitTopLevel(inner).map(_.trim.stripPrefix("\"").stripSuffix("\"").stripPrefix("'").stripSuffix("'")).toList
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
   * @param input The raw value from JSON/CSV/maps.
   * @return the parsed [[RuleValue]], or `None` for null/empty/"NULL"
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
      parseValue(s.stripPrefix("LongValue(").stripSuffix(")")) match {
        case Some(l: LongValue) => Some(l)
        case _                  => throw new SumehException(s"Malformed tagged rule value: '$s'")
      }
    case s: String if s.startsWith("DoubleValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DoubleValue(").stripSuffix(")")) match {
        case Some(d: DoubleValue) => Some(d)
        case _                    => throw new SumehException(s"Malformed tagged rule value: '$s'")
      }
    case s: String if s.startsWith("BoolValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("BoolValue(").stripSuffix(")")) match {
        case Some(b: BoolValue) => Some(b)
        case _                  => throw new SumehException(s"Malformed tagged rule value: '$s'")
      }
    case s: String if s.startsWith("DateValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DateValue(").stripSuffix(")")) match {
        case Some(d: DateValue) => Some(d)
        case _                  => throw new SumehException(s"Malformed tagged rule value: '$s'")
      }
    case s: String if s.startsWith("DateTimeValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("DateTimeValue(").stripSuffix(")")) match {
        case Some(dt: DateTimeValue) => Some(dt)
        case _                       => throw new SumehException(s"Malformed tagged rule value: '$s'")
      }
    case s: String if s.startsWith("ListValue(") && s.endsWith(")") =>
      parseValue(s.stripPrefix("ListValue(").stripSuffix(")")) match {
        case Some(l: ListValue) => Some(l)
        case _                  => throw new SumehException(s"Malformed tagged rule value: '$s'")
      }
    case s: String =>
      val trimmed = s.trim
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        val inner = trimmed.drop(1).dropRight(1)
        val items = splitTopLevel(inner)
          .map(_.trim.stripPrefix("\"").stripSuffix("\"").stripPrefix("'").stripSuffix("'"))
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
   * @param input The raw value — a `LocalDateTime`, or a string parseable by `LocalDateTime.parse`.
   * @return the parsed [[LocalDateTime]], or `None` on failure
   */
  private def parseTimestamp(input: Any): Option[LocalDateTime] = input match {
    case dt: LocalDateTime => Some(dt)
    case s: String         => Try(LocalDateTime.parse(s)).toOption
    case _                 => None
  }

  /**
   * Splits a string on commas at nesting depth zero.
   *
   * Commas inside `[...]`, `(...)`, or quoted sections are left intact, so a nested `ListValue([...])` or a
   * `StringValue` containing a comma survives as a single element instead of being mis-split.
   *
   * @param s The string to split.
   * @return the string split into tokens at nesting depth zero
   */
  private def splitTopLevel(s: String): List[String] = {
    val out   = scala.collection.mutable.ListBuffer[String]()
    val buf   = new StringBuilder
    var depth = 0
    var quote = '\u0000'
    s.foreach {
      case c if quote != '\u0000' =>
        if (c == quote) quote = '\u0000'
        buf += c
      case c @ ('"' | '\'')  => quote = c; buf += c
      case c @ ('[' | '(')   => depth += 1; buf += c
      case c @ (']' | ')')   => depth -= 1; buf += c
      case ',' if depth == 0 => out += buf.toString; buf.clear()
      case c                 => buf += c
    }
    if (buf.nonEmpty) out += buf.toString
    out.toList
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
   * @return a lossless serialized form usable for CSV round-tripping
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
   * @param v The rule value.
   * @return the rule value converted to a plain JVM literal for use with `lit`/`expr`
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
   * @return the value wrapped in its constructor name, e.g. LongValue(42)
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
   * @return the value wrapped in its constructor name, e.g. LongValue(42)
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
   * @return the value wrapped in its constructor name, e.g. LongValue(42)
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
   * @return the value wrapped in its constructor name, e.g. LongValue(42)
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
   * @return the value wrapped in its constructor name, e.g. LongValue(42)
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
   * @return the value wrapped in its constructor name, e.g. LongValue(42)
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
   * @return the value wrapped in its constructor name, e.g. LongValue(42)
   */
  def toTaggedString: String = s"ListValue([${v.map(_.toTaggedString).mkString(",")}])"
}
