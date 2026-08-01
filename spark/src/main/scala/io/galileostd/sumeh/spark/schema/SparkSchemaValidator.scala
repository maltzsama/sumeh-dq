package io.galileostd.sumeh.spark.schema

import io.galileostd.sumeh.schema.{ ColumnDef, SchemaDef, SchemaReport }
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

/**
 * Schema validator for Spark DataFrames.
 *
 * Extracts the actual schema of a DataFrame and compares it against an expected `SchemaDef` contract, reporting missing
 * columns, type mismatches, comment/nullability violations, and (optionally) extra columns. Mirrors Python's
 * `validate_schema` / `extract_schema` for the Spark engine.
 */
object SparkSchemaValidator {

  // -------------------------------------------------------------------------
  // Type mapping — Spark DataType → canonical type
  // -------------------------------------------------------------------------

  /**
   * Canonicalization map: Spark type names → canonical contract types.
   *
   * Numeric variants (byte, tinyint, short, smallint, ...) are folded into their canonical form so the contract does
   * not depend on engine-specific spellings.
   */
  private val typeMap: Map[String, String] = Map(
    "byte"          -> "integer",
    "tinyint"       -> "integer",
    "short"         -> "integer",
    "smallint"      -> "integer",
    "integer"       -> "integer",
    "int"           -> "integer",
    "long"          -> "integer",
    "bigint"        -> "integer",
    "float"         -> "float",
    "double"        -> "float",
    "decimal"       -> "float",
    "string"        -> "string",
    "varchar"       -> "string",
    "char"          -> "string",
    "binary"        -> "binary",
    "boolean"       -> "boolean",
    "date"          -> "datetime",
    "timestamp"     -> "datetime",
    "timestamp_ntz" -> "datetime",
    "array"         -> "array",
    "struct"        -> "complex",
    "map"           -> "complex",
    "datetime"      -> "datetime",
    "complex"       -> "complex"
  )

  /**
   * Maps a Spark [[DataType]] to its canonical string form.
   *
   * Numeric types collapse to `integer`/`float`, temporal types to `datetime`, and struct/map to `complex`, so a
   * contract written in portable terms (`"integer"`, `"datetime"`, ...) compares against Spark's concrete types.
   *
   * Args: dt: The Spark data type.
   *
   * Returns: The canonical type name, or `"unknown"` for unmapped types.
   */
  private def toCanonical(dt: DataType): String = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType => "integer"
    case _: FloatType | _: DoubleType | _: DecimalType             => "float"
    case _: StringType                                             => "string"
    case _: BinaryType                                             => "binary"
    case _: BooleanType                                            => "boolean"
    case _: DateType | _: TimestampType | _: TimestampNTZType      => "datetime"
    case _: ArrayType                                              => "array"
    case _: StructType | _: MapType                                => "complex"
    case _                                                         => "unknown"
  }

  // -------------------------------------------------------------------------
  // Extract schema from DataFrame
  // -------------------------------------------------------------------------

  /**
   * Extracts the actual schema of a DataFrame as a `Map[colName -> info]`.
   *
   * Each info map holds `raw_type`, `nullable`, and `comment`, plus `element_type` (arrays) or `nested_fields`
   * (structs) where applicable.
   *
   * Args: df: The DataFrame.
   *
   * Returns: Column name → schema info.
   */
  def extractSchema(df: DataFrame): Map[String, Map[String, Any]] =
    df.schema.fields.map {
      field =>
        val info = scala.collection.mutable.Map[String, Any](
          "raw_type"       -> field.dataType.typeName,
          "canonical_type" -> toCanonical(field.dataType),
          "nullable"       -> field.nullable,
          "comment"        -> (if (field.metadata.contains("comment")) field.metadata.getString("comment") else "")
        )

        field.dataType match {
          case at: ArrayType if at.elementType.isInstanceOf[StructType] =>
            info("nested_fields") = extractSchemaFromStructType(at.elementType.asInstanceOf[StructType])
          case at: ArrayType =>
            info("element_type") = toCanonical(at.elementType)
          case st: StructType =>
            info("nested_fields") = extractSchemaFromStructType(st)
          case _ =>
        }

        field.name -> info.toMap
    }.toMap

  /**
   * Extracts a nested struct's fields as a `colName -> info` map.
   *
   * Args: st: The struct type.
   *
   * Returns: Nested field name → basic info (`raw_type`, `nullable`, empty `comment`).
   */
  private def extractSchemaFromStructType(st: StructType): Map[String, Map[String, Any]] =
    st.fields.map {
      f =>
        f.name -> Map[String, Any](
          "raw_type"       -> f.dataType.typeName,
          "canonical_type" -> toCanonical(f.dataType),
          "nullable"       -> f.nullable,
          "comment"        -> ""
        )
    }.toMap

  // -------------------------------------------------------------------------
  // Validate schema
  // -------------------------------------------------------------------------

  /**
   * Validates a Spark DataFrame against an expected `SchemaDef`.
   *
   * Compares types, nullability, comments, array element types, and nested struct fields. When `strictColumns` is set
   * on the contract, columns present in the data but absent from the contract are reported as `extraCols`.
   *
   * Args: df: The DataFrame. expected: The schema contract.
   *
   * Returns: A `SchemaReport` summarizing the outcome.
   */
  def validate(df: DataFrame, expected: SchemaDef): SchemaReport = {
    val actual = extractSchema(df)
    val report = validateRecursive(expected.columns, actual)

    if (expected.strictColumns) {
      val expectedNames = expected.columns.map(_.name).toSet
      val extra         = actual.keys.filterNot(expectedNames.contains).toList
      if (extra.nonEmpty)
        return report.copy(passed = false, extraCols = extra)
    }

    report
  }

  /**
   * Recursively compares expected columns against actual schema info, collecting issues.
   *
   * For each expected column it checks presence, canonical type, array element type, comment, nullability, and nested
   * fields, merging child reports into a single one. Missing optional columns are ignored.
   *
   * Args: expectedCols: The contract columns. actualCols: The extracted schema info, keyed by column name. parentPath:
   * Dot-prefix for nested column names in messages (e.g. `"address.street"`).
   *
   * Returns: A [[io.galileostd.sumeh.schema.SchemaReport]] with the collected issues.
   */
  private def validateRecursive(
      expectedCols: List[ColumnDef],
      actualCols: Map[String, Map[String, Any]],
      parentPath: String = ""
  ): SchemaReport = {

    val missingCols    = scala.collection.mutable.ListBuffer[String]()
    val typeErrors     = scala.collection.mutable.Map[String, String]()
    val metadataErrors = scala.collection.mutable.Map[String, String]()

    for (colDef <- expectedCols) {
      val fullPath = if (parentPath.isEmpty) colDef.name else s"$parentPath.${colDef.name}"
      actualCols.get(colDef.name) match {

        case None =>
          if (!colDef.isOptional) missingCols += fullPath

        case Some(actual) =>
          // Type check
          val canonExpected = canonExpectedType(colDef.expectedType)
          val canonActual   = actual.getOrElse("canonical_type", "unknown").toString

          if (canonExpected == "unknown") {
            typeErrors(fullPath) = s"Unknown expected type '${colDef.expectedType}'. " +
              s"Valid: ${typeMap.keys.toList.sorted.mkString(", ")}"
          } else if (canonActual == "unknown") {
            typeErrors(fullPath) = s"Spark type '${actual.getOrElse("raw_type", "?")}' has no canonical mapping"
          } else if (canonExpected != canonActual) {
            typeErrors(fullPath) = s"Expected $canonExpected, got $canonActual (${actual.getOrElse("raw_type", "?")})"
          } else {
            // Element type (array)
            for {
              expectedElem <- colDef.elementType
              actualElem   <- actual.get("element_type").map(_.toString)
            }
              if (expectedElem.toLowerCase != actualElem.toLowerCase)
                typeErrors(fullPath) = s"Array element type mismatch: expected $expectedElem, got $actualElem"

            // Comment validation
            val actualComment = actual.getOrElse("comment", "").toString.trim
            if (colDef.requireComment && actualComment.isEmpty)
              metadataErrors(fullPath) = "Missing required column comment"
            colDef.expectedComment.foreach {
              expected =>
                if (expected != actualComment)
                  metadataErrors(fullPath) = s"Comment mismatch. Expected: '$expected'"
            }

            // Nullability
            val actualNullable = actual.getOrElse("nullable", true).asInstanceOf[Boolean]
            if (!colDef.nullable && actualNullable)
              metadataErrors(fullPath) = "Schema violation: contract requires NOT NULL but column allows nulls"

            // Nested fields
            for {
              nestedDefs   <- colDef.fields
              nestedActual <- actual.get("nested_fields").map(_.asInstanceOf[Map[String, Map[String, Any]]])
            } {
              val childReport = validateRecursive(nestedDefs, nestedActual, fullPath)
              missingCols ++= childReport.missingCols
              typeErrors ++= childReport.typeErrors
              metadataErrors ++= childReport.metadataErrors
            }
          }
      }
    }

    val passed = missingCols.isEmpty && typeErrors.isEmpty && metadataErrors.isEmpty
    SchemaReport(
      passed = passed,
      missingCols = missingCols.toList,
      typeErrors = typeErrors.toMap,
      metadataErrors = metadataErrors.toMap
    )
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Normalizes an expected type so it compares against the canonical actual type.
   *
   * `varchar(10)`, `decimal(10,2)` etc. are trimmed before the parameter list, then looked up in `typeMap`. Unknown
   * names return `"unknown"` so the caller can report a misconfigured contract instead of silently skipping the check.
   *
   * Args: t: The contract type string.
   *
   * Returns: The canonical type, or `"unknown"` when not recognized.
   */
  private def canonExpectedType(t: String): String = {
    val low  = t.toLowerCase.trim
    val base = low.takeWhile(c => c != '(' && c != '<')
    typeMap.getOrElse(base, "unknown")
  }
}
