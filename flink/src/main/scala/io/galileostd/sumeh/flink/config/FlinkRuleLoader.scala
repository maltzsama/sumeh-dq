package io.galileostd.sumeh.flink.config

import scala.jdk.CollectionConverters._

import io.galileostd.sumeh.config.RuleLoader
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.flink.table.api.{ Table, TableEnvironment, TableResult }
import org.apache.flink.types.Row

/**
 * Loads `RuleDefinition`s from Flink Tables and TableResults.
 *
 * Required columns: `field`, `check_type`. Optional: `value`, `threshold`, `execute`, `level`, `category`. Extra
 * columns are preserved as metadata.
 */
object FlinkRuleLoader {

  /**
   * Collects all rows of a TableResult and closes the iterator.
   *
   * @param result
   *   The TableResult to drain.
   * @return
   *   A List of all collected rows.
   */
  private def drain(result: TableResult): List[Row] = {
    val it = result.collect()
    try {
      val buf = List.newBuilder[Row]
      while (it.hasNext) buf += it.next()
      buf.result()
    } finally it.close()
  }

  /**
   * Load rules from a Flink Table.
   *
   * @param table
   *   A Flink Table (already registered or from a query).
   * @return
   *   List of RuleDefinitions.
   * @throws java.lang.IllegalArgumentException
   *   if `field` or `check_type` columns are missing.
   */
  def fromTable(table: Table): List[RuleDefinition] = {
    val fieldNames = table.getResolvedSchema.getColumnNames.asScala.toArray

    val required = Set("field", "check_type")
    val cols     = fieldNames.toSet
    val missing  = required -- cols
    require(missing.isEmpty, s"Missing required columns: ${missing.mkString(", ")}")

    val rows = drain(table.execute())

    rows.map {
      row =>
        val map = fieldNames.zipWithIndex.map {
          case (name, idx) =>
            val value = row.getField(idx)
            name -> (if (value != null) value.toString else "")
        }.toMap
        RuleDefinition.fromMap(map)
    }
  }

  /**
   * Load rules from a Flink Table that has a single JSON column.
   *
   * @param table
   *   The source table (must have a column with JSON).
   * @param column
   *   The column name containing JSON.
   * @param tableEnv
   *   The TableEnvironment (needed for sqlQuery).
   * @return
   *   List of RuleDefinitions parsed from each JSON row.
   */
  def fromJsonColumn(
      table: Table,
      column: String = "config",
      tableEnv: TableEnvironment
  ): List[RuleDefinition] = {
    val columns = table.getResolvedSchema.getColumnNames.asScala.toSet
    require(
      columns.contains(column),
      s"Column '$column' not found in table schema: ${columns.toList.sorted.mkString(", ")}"
    )

    val viewName = s"dq_rules_${System.nanoTime()}"
    tableEnv.createTemporaryView(viewName, table)

    try {
      val sql      = s"SELECT `$column` FROM `$viewName`"
      val selected = tableEnv.sqlQuery(sql)
      val rows     = drain(selected.execute())

      rows.flatMap {
        row =>
          val json = Option(row.getField(0)).map(_.toString).getOrElse("")
          RuleLoader.fromJsonString(json)
      }
    } finally
      tableEnv.dropTemporaryView(viewName)
  }

  /**
   * Load rules from a TableResult (e.g., from executeSql).
   *
   * @param result
   *   TableResult from a query.
   * @param fieldNames
   *   The field names (must be provided, because TableResult may not expose them cleanly).
   * @return
   *   List of RuleDefinitions.
   * @throws java.lang.IllegalArgumentException
   *   if `field` or `check_type` columns are missing.
   */
  def fromTableResult(
      result: TableResult,
      fieldNames: Array[String]
  ): List[RuleDefinition] = {
    val required = Set("field", "check_type")
    val cols     = fieldNames.toSet
    val missing  = required -- cols
    require(missing.isEmpty, s"Missing required columns: ${missing.mkString(", ")}")

    val rows = drain(result)

    rows.map {
      row =>
        val map = fieldNames.zipWithIndex.map {
          case (name, idx) =>
            val value = row.getField(idx)
            name -> (if (value != null) value.toString else "")
        }.toMap
        RuleDefinition.fromMap(map)
    }
  }
}
