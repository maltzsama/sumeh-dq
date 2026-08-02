package io.galileostd.sumeh.schema

/**
 * Definition of a single column in a schema contract.
 *
 * Describes the expected shape of one column — its canonical type, nullability, whether it is required, and (for
 * complex types) its element or nested fields.
 *
 * Args: name: Column name. expectedType: Expected canonical type (e.g. `"string"`, `"integer"`, `"float"`, `"array"`,
 * `"struct"`). isOptional: When `true`, a missing column is not reported as an error. nullable: Whether the column may
 * contain nulls. elementType: For array columns, the element type (e.g. `"string"`, `"integer"`). requireComment:
 * Whether a comment/description is required on the column. expectedComment: The exact comment text expected on the
 * column. fields: Nested [[ColumnDef]]s for struct columns.
 */
final case class ColumnDef(
    name: String,
    expectedType: String,
    isOptional: Boolean = false,
    nullable: Boolean = true,
    elementType: Option[String] = None,
    requireComment: Boolean = false,
    expectedComment: Option[String] = None,
    fields: Option[List[ColumnDef]] = None
)

/**
 * Companion with a Map-based constructor.
 */
object ColumnDef {

  /**
   * Creates a [[ColumnDef]] from a property map.
   *
   * Accepts either a plain type string (e.g. `"string"`) or a map with the keys `type`, `is_optional`, `nullable`,
   * `element_type`, `require_comment`, `expected_comment`, and `fields` (recursively parsed). Mirrors Python's
   * `from_dict`.
   *
   * Args: name: The column name. props: The type string or property map.
   *
   * Returns: The parsed column definition.
   */
  def fromMap(name: String, props: Any): ColumnDef = props match {
    case s: String => ColumnDef(name = name, expectedType = s)
    case m: Map[_, _] =>
      val map    = m.asInstanceOf[Map[String, Any]]
      val nested = map.get("fields").flatMap(parseFields)
      ColumnDef(
        name = name,
        expectedType = map.getOrElse("type", "string").toString,
        isOptional = asBool(map.getOrElse("is_optional", false)),
        nullable = asBool(map.getOrElse("nullable", true)),
        elementType = map.get("element_type").map(_.toString),
        requireComment = asBool(map.getOrElse("require_comment", false)),
        expectedComment = map.get("expected_comment").map(_.toString),
        fields = nested
      )
    case _ => ColumnDef(name = name, expectedType = "string")
  }

  /**
   * Parses a nested `fields` payload into column definitions.
   *
   * Accepts either a name-keyed map (e.g. `Map("street" -> "string")`) or a list of field objects, each carrying a
   * `name` key alongside its type contract (e.g. `List(Map("name" -> "street", "type" -> "string"))`). Any other shape
   * yields `None`, so the caller treats it as "no nested fields declared" rather than an empty-but-present list.
   *
   * Args: payload: The raw `fields` value.
   *
   * Returns: The parsed nested columns, or `None` for an unrecognized shape.
   *
   * Throws: IllegalArgumentException if a list entry is not a map with a usable `name`.
   */
  private def parseFields(payload: Any): Option[List[ColumnDef]] = payload match {
    case f: Map[_, _] =>
      Some(f.asInstanceOf[Map[String, Any]].map { case (k, v) => fromMap(k.toString, v) }.toList)
    case f: Seq[_] =>
      Some(
        f.toList.map {
          case item: Map[_, _] =>
            val m    = item.asInstanceOf[Map[String, Any]]
            val name = m.getOrElse("name", "").toString.trim
            if (name.isEmpty)
              throw new IllegalArgumentException("Invalid nested field entry: missing 'name'")
            fromMap(name, m - "name")
          case other =>
            throw new IllegalArgumentException(s"Invalid nested field entry: $other")
        }
      )
    case _ => None
  }

  /**
   * Lenient Boolean coercion for config values.
   *
   * Accepts `Boolean`, the strings `"true"/"1"/"yes"/"y"/"t"` (case-insensitive), and non-zero numbers.
   *
   * Args: v: The raw config value.
   *
   * Returns: The coerced boolean.
   */
  private def asBool(v: Any): Boolean = v match {
    case b: Boolean => b
    case s: String  => Set("true", "1", "yes", "y", "t").contains(s.trim.toLowerCase)
    case n: Number  => n.doubleValue() != 0
    case null       => false
    case _          => false
  }
}

/**
 * Schema definition — a list of column contracts.
 *
 * A `SchemaDef` is the expected shape of a DataFrame, used by the schema validator to compare against reality.
 *
 * Args: columns: The column contracts. strictColumns: When `true`, extra columns in the DataFrame are reported as
 * errors.
 */
final case class SchemaDef(
    columns: List[ColumnDef],
    strictColumns: Boolean = false
)

/**
 * Companion with a Map-based constructor.
 */
object SchemaDef {

  /**
   * Builds a [[SchemaDef]] from a column-name → contract map.
   *
   * Each value is either a plain type string or a property map, passed to [[ColumnDef.fromMap]].
   *
   * Args: data: Map of column name to contract. strict: When `true`, extra columns in the DataFrame become errors.
   *
   * Returns: The resulting schema definition.
   */
  def fromMap(data: Map[String, Any], strict: Boolean = false): SchemaDef =
    SchemaDef(
      columns = data.map { case (k, v) => ColumnDef.fromMap(k, v) }.toList,
      strictColumns = strict
    )
}

/**
 * Schema validation report.
 *
 * Summarizes the outcome of a schema validation run against a [[SchemaDef]].
 *
 * Args: passed: Whether validation passed (no missing, type, or metadata issues). missingCols: Required columns that
 * were not found in the data. typeErrors: Columns with the wrong type — column name → error message. metadataErrors:
 * Columns with comment or nullability issues — column name → message. extraCols: Columns present in the data but not in
 * the schema (only when `strictColumns` is `true`).
 */
final case class SchemaReport(
    passed: Boolean,
    missingCols: List[String] = List.empty,
    typeErrors: Map[String, String] = Map.empty,
    metadataErrors: Map[String, String] = Map.empty,
    extraCols: List[String] = List.empty
) {

  /**
   * Total number of issues across all categories.
   *
   * Returns: The sum of missing columns, type errors, metadata errors, and extra columns.
   */
  def totalIssues: Int =
    missingCols.size + typeErrors.size + metadataErrors.size + extraCols.size

  /**
   * Flat map form of the report.
   *
   * Keys: `passed`, `missing_columns`, `type_errors`, `metadata_errors`, `extra_columns`, `total_issues`.
   *
   * Returns: The report as a serializable map.
   */
  def toMap: Map[String, Any] = Map(
    "passed"          -> passed,
    "missing_columns" -> missingCols,
    "type_errors"     -> typeErrors,
    "metadata_errors" -> metadataErrors,
    "extra_columns"   -> extraCols,
    "total_issues"    -> totalIssues
  )

  /**
   * Compact rendering of the report outcome.
   *
   * Returns: A string like `SchemaReport(✓ PASSED, 0 issues)`.
   */
  override def toString: String = {
    val status = if (passed) "✓ PASSED" else "✗ FAILED"
    s"SchemaReport($status, $totalIssues issues)"
  }
}
