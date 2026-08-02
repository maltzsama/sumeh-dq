package io.galileostd.sumeh.config

import scala.util.Try

import io.galileostd.sumeh.exception.SumehException
import io.galileostd.sumeh.rule.RuleDefinition

/**
 * Load and export [[io.galileostd.sumeh.rule.RuleDefinition]] lists from/to strings.
 *
 * This is a pure parsing layer — no I/O, no disk, no S3, no GCS, no Azure, no DBFS. The caller is responsible for
 * reading the raw text from wherever it lives (S3, GCS, DBFS, JDBC, HTTP, local file, ...) and passing it to
 * `fromCsvString` / `fromJsonString`; `toCsv` / `toJson` return strings the caller can persist anywhere.
 */
object RuleLoader {

  // -------------------------------------------------------------------------
  // LOAD from strings
  // -------------------------------------------------------------------------

  /**
   * Parses rules from a CSV string.
   *
   * Expects a header row with the columns `field,check_type,value,threshold,tolerance,execute,level,category`. Each
   * subsequent non-empty line becomes one rule via [[io.galileostd.sumeh.rule.RuleDefinition.fromMap]]; `value` may use
   * the lossless tagged format (e.g. `LongValue(42)`) from [[toCsv]]. Missing columns — including a CSV written before
   * `tolerance` existed — fall back to `RuleDefinition`'s defaults.
   *
   * Args: csv: The raw CSV text, with or without a trailing newline.
   *
   * Returns: The parsed rules; an empty list for an empty or header-only input.
   *
   * Throws: [[io.galileostd.sumeh.exception.SumehException]] if a row has an unknown or missing `check_type`.
   */
  def fromCsvString(csv: String): List[RuleDefinition] = {
    val lines = csv.linesIterator.toList
    if (lines.isEmpty) return List.empty

    val header = parseCsvLine(lines.head)
    lines.tail
      .filter(_.trim.nonEmpty)
      .map {
        line =>
          val values = parseCsvLine(line)
          val row    = header.zipAll(values, "", "").toMap
          RuleDefinition.fromMap(row.filter(_._2.nonEmpty))
      }
  }

  /**
   * Parses rules from a JSON string.
   *
   * Accepts either a single rule object or an array of rule objects. Values are flattened to strings with
   * `jsonValueToString` before going through [[io.galileostd.sumeh.rule.RuleDefinition.fromMap]], so a JSON `value` of
   * `[18, 65]` becomes the list-string `"[18,65]"` and is re-parsed by `parseValue`.
   *
   * Args: json: The raw JSON text.
   *
   * Returns: The parsed rules; an empty list for null/empty input or non-object JSON.
   *
   * Throws: [[io.galileostd.sumeh.exception.SumehException]] if a rule has an unknown or missing `check_type`.
   */
  def fromJsonString(json: String): List[RuleDefinition] = {
    import upickle.default._

    if (json == null || json.trim.isEmpty) return List.empty

    val parsed = Try(ujson.read(json)).getOrElse(ujson.Null)

    val rows: List[ujson.Obj] = parsed match {
      case arr: ujson.Arr => arr.value.toList.collect { case o: ujson.Obj => o }
      case obj: ujson.Obj => List(obj)
      case _              => List.empty
    }

    rows.map {
      row =>
        val strMap = row.value.map { case (k, v) => k -> jsonValueToString(v) }.toMap
        RuleDefinition.fromMap(strMap)
    }
  }

  // -------------------------------------------------------------------------
  // EXPORT to strings
  // -------------------------------------------------------------------------

  /**
   * Serializes rules to a CSV string.
   *
   * Writes a header row followed by one line per rule. `value` uses the lossless tagged `RuleValue.toTaggedString`
   * format so numbers, booleans, and dates survive the round-trip through [[fromCsvString]].
   *
   * Args: rules: The rules to serialize.
   *
   * Returns: A CSV string with a header row.
   */
  def toCsv(rules: List[RuleDefinition]): String = {
    val header = "field,check_type,value,threshold,tolerance,execute,level,category"
    val lines = rules.map {
      r =>
        val value     = r.value.map(_.toTaggedString).getOrElse("")
        val threshold = r.threshold.toString
        val tolerance = r.tolerance.toString
        val execute   = r.execute.toString
        val level     = r.level
        val category  = r.category
        List(
          quoteCsv(r.fieldName),
          quoteCsv(r.checkType),
          quoteCsv(value),
          quoteCsv(threshold),
          quoteCsv(tolerance),
          quoteCsv(execute),
          quoteCsv(level),
          quoteCsv(category)
        ).mkString(",")
    }
    (header +: lines).mkString("\n")
  }

  /**
   * Serializes rules to a JSON array string.
   *
   * Each rule becomes an object with `field`, `check_type`, `threshold`, `execute`, `level`, `category`, plus `value`
   * (in its native JSON form), `updated_at`, and any `metadata` keys. Note: `value` and metadata are stringified, so
   * this is a lossless-but-not-typed export.
   *
   * Args: rules: The rules to serialize.
   *
   * Returns: A JSON array string.
   */
  def toJson(rules: List[RuleDefinition]): String = {
    val arr = rules.map {
      r =>
        val base = Map(
          "field"      -> ujson.Str(r.fieldName),
          "check_type" -> ujson.Str(r.checkType),
          "threshold"  -> ujson.Num(r.threshold),
          "tolerance"  -> ujson.Num(r.tolerance),
          "execute"    -> ujson.Bool(r.execute),
          "level"      -> ujson.Str(r.level),
          "category"   -> ujson.Str(r.category)
        ) ++
          r.value.map(v => "value" -> ruleValueToJson(v)).toMap ++
          r.updatedAt.map(dt => "updated_at" -> ujson.Str(dt.toString)).toMap

        // Metadata: converte Any pra string
        val metadataJson = r.metadata.map {
          case (k, v) =>
            k -> ujson.Str(v.toString)
        }

        ujson.Obj.from(base ++ metadataJson)
    }

    ujson.write(ujson.Arr.from(arr))
  }

  // -------------------------------------------------------------------------
  // CSV parsing (internal)
  // -------------------------------------------------------------------------

  /**
   * Parses a single CSV line into fields.
   *
   * Honors quoted fields: a field wrapped in double quotes may contain commas, and `""` is treated as a literal quote.
   * Embedded newlines are not supported — the input is split into physical lines by [[fromCsvString]] before this
   * function ever sees a line, so a newline inside a quoted field breaks the record in two. Whitespace around unquoted
   * fields is trimmed.
   *
   * Args: line: The raw line.
   *
   * Returns: The parsed field values, in order.
   *
   * Throws: [[io.galileostd.sumeh.exception.SumehException]] if the line ends with an unterminated quote — the signal
   * that a quoted field's newline was split across physical lines.
   */
  private def parseCsvLine(line: String): List[String] = {
    val result  = scala.collection.mutable.ListBuffer[String]()
    val current = new StringBuilder
    var inQuote = false
    var i       = 0

    while (i < line.length) {
      val ch = line.charAt(i)

      if (inQuote) {
        if (ch == '"') {
          if (i + 1 < line.length && line.charAt(i + 1) == '"') {
            current.append('"')
            i += 2
          } else {
            inQuote = false
            i += 1
          }
        } else {
          current.append(ch)
          i += 1
        }
      } else {
        ch match {
          case '"' =>
            inQuote = true
            i += 1
          case ',' =>
            result += current.toString.trim
            current.clear()
            i += 1
          case _ =>
            current.append(ch)
            i += 1
        }
      }
    }

    if (inQuote)
      throw new SumehException(
        s"Unterminated quoted field in CSV line: $line. " +
          "If this field is meant to contain a newline, quoted multi-line fields are not supported — " +
          "encode the value with an escape (e.g. \\n) instead of a literal line break."
      )

    result += current.toString.trim
    result.toList
  }

  /**
   * Quotes a CSV field only when it needs quoting.
   *
   * A field is wrapped in double quotes (with internal quotes doubled) when it contains a comma, a quote, or a newline;
   * otherwise it is returned unchanged.
   *
   * Args: field: The raw field value.
   *
   * Returns: The field, quoted if necessary.
   */
  private def quoteCsv(field: String): String = {
    val needsQuoting = field.contains(",") || field.contains("\"") || field.contains("\n")
    if (needsQuoting) {
      val escaped = field.replace("\"", "\"\"")
      s""""$escaped""""
    } else field
  }

  // -------------------------------------------------------------------------
  // JSON helpers (internal)
  // -------------------------------------------------------------------------

  /**
   * Flattens a ujson value to a plain string for [[io.galileostd.sumeh.rule.RuleDefinition.fromMap]].
   *
   * Numbers are rendered as integers when whole (e.g. `42` instead of `42.0`). Arrays and objects are flattened to the
   * compact `"[a,b]"` / `"{k:v}"` forms that `parseField`/`parseValue` understand.
   *
   * Args: v: The JSON value.
   *
   * Returns: Its string representation.
   */
  private def jsonValueToString(v: ujson.Value): String = v match {
    case ujson.Str(s)   => s
    case ujson.Num(n)   => if (n == n.toLong) n.toLong.toString else n.toString
    case ujson.Bool(b)  => b.toString
    case ujson.Null     => ""
    case ujson.Arr(arr) => arr.map(jsonValueToString).mkString("[", ",", "]")
    case ujson.Obj(obj) => obj.map { case (k, v) => s"$k:${jsonValueToString(v)}" }.mkString("{", ",", "}")
  }

  /**
   * Converts a [[RuleValue]] to its native ujson representation.
   *
   * Dates/timestamps become strings, lists become arrays, and numerics stay numeric — the inverse of
   * [[io.galileostd.sumeh.rule.RuleDefinition.parseValue]].
   *
   * Args: v: The rule value.
   *
   * Returns: The corresponding ujson value.
   */
  private def ruleValueToJson(v: io.galileostd.sumeh.rule.RuleValue): ujson.Value = v match {
    case io.galileostd.sumeh.rule.StringValue(s)    => ujson.Str(s)
    case io.galileostd.sumeh.rule.LongValue(l)      => ujson.Num(l.toDouble)
    case io.galileostd.sumeh.rule.DoubleValue(d)    => ujson.Num(d)
    case io.galileostd.sumeh.rule.BoolValue(b)      => ujson.Bool(b)
    case io.galileostd.sumeh.rule.DateValue(d)      => ujson.Str(d.toString)
    case io.galileostd.sumeh.rule.DateTimeValue(dt) => ujson.Str(dt.toString)
    case io.galileostd.sumeh.rule.ListValue(items)  => ujson.Arr.from(items.map(ruleValueToJson))
  }
}
