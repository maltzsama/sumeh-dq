package io.galileostd.sumeh.schema

/**
 * Definition of a single column in a schema contract.
 *
 * Args: name: Column name. expectedType: Expected canonical type ("string", "integer", "float", etc). isOptional: If
 * true, a missing column is not an error. nullable: Whether the column allows nulls. elementType: For array columns:
 * element type (e.g. "string", "integer"). requireComment: Whether a comment/description is required. expectedComment:
 * Expected comment text. fields: Nested columns for struct types.
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

/** Companion with a Map-based constructor. */
object ColumnDef {

  /** Create from a Map — mirrors Python's from_dict. */
  def fromMap(name: String, props: Any): ColumnDef = props match {
    case s: String => ColumnDef(name = name, expectedType = s)
    case m: Map[_, _] =>
      val map = m.asInstanceOf[Map[String, Any]]
      val nested = map.get("fields").map {
        case f: Map[_, _] =>
          f.asInstanceOf[Map[String, Any]].map { case (k, v) => fromMap(k.toString, v) }.toList
        case _ => List.empty
      }
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

  /** Lenient Boolean coercion (accepts Boolean, "true"/"yes"/"1", and non-zero numbers). */
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
 * Args: columns: Column definitions. strictColumns: If true, extra columns in the DataFrame are errors.
 */
final case class SchemaDef(
    columns: List[ColumnDef],
    strictColumns: Boolean = false
)

/** Companion with a Map-based constructor. */
object SchemaDef {

  /**
   * Builds a SchemaDef from a column-name -> contract map.
   *
   * Args: data: Map of column name to a type string or a property map. strict: When true, extra columns become errors.
   *
   * Returns: The resulting SchemaDef.
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
 * Args: passed: Whether validation passed. missingCols: Columns required but not found. typeErrors: Columns with wrong
 * type — colName -> error message. metadataErrors: Columns with comment/nullability issues. extraCols: Columns present
 * but not in the schema (only when strictColumns=true).
 */
final case class SchemaReport(
    passed: Boolean,
    missingCols: List[String] = List.empty,
    typeErrors: Map[String, String] = Map.empty,
    metadataErrors: Map[String, String] = Map.empty,
    extraCols: List[String] = List.empty
) {

  /** Total number of issues across all categories. */
  def totalIssues: Int =
    missingCols.size + typeErrors.size + metadataErrors.size + extraCols.size

  /** Flat map form of the report. */
  def toMap: Map[String, Any] = Map(
    "passed"          -> passed,
    "missing_columns" -> missingCols,
    "type_errors"     -> typeErrors,
    "metadata_errors" -> metadataErrors,
    "extra_columns"   -> extraCols,
    "total_issues"    -> totalIssues
  )

  override def toString: String = {
    val status = if (passed) "✓ PASSED" else "✗ FAILED"
    s"SchemaReport($status, $totalIssues issues)"
  }
}
