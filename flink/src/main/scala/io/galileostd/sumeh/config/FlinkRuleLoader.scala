package io.galileostd.sumeh.flink.config

import io.galileostd.sumeh.config.RuleLoader
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.flink.table.api.{ Table, TableEnvironment, TableResult }
import org.apache.flink.types.Row

object FlinkRuleLoader {

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
   *   A Flink Table (already registered or from a query)
   * @return
   *   List of RuleDefinition
   */
  def fromTable(table: Table): List[RuleDefinition] = {
    val fieldNames = table.getSchema.getFieldNames

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
   *   The source table (must have a column with JSON)
   * @param column
   *   The column name containing JSON
   * @param tableEnv
   *   The TableEnvironment (needed for sqlQuery)
   * @return
   *   List of RuleDefinition
   */
  def fromJsonColumn(
      table: Table,
      column: String = "config",
      tableEnv: TableEnvironment
  ): List[RuleDefinition] = {
    val viewName = s"dq_rules_${System.nanoTime()}"
    tableEnv.createTemporaryView(viewName, table)

    val sql      = s"SELECT `$column` FROM `$viewName`"
    val selected = tableEnv.sqlQuery(sql)
    val rows     = drain(selected.execute())

    rows.flatMap {
      row =>
        val json = Option(row.getField(0)).map(_.toString).getOrElse("")
        RuleLoader.fromJsonString(json)
    }
  }

  /**
   * Load rules from a TableResult (e.g., from executeSql).
   *
   * @param result
   *   TableResult from a query
   * @param fieldNames
   *   The field names (must be provided, because TableResult may not expose them cleanly)
   * @return
   *   List of RuleDefinition
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
