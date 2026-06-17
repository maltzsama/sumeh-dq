package io.galileostd.sumeh.flink.config

import scala.jdk.CollectionConverters._

import io.galileostd.sumeh.config.RuleLoader
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.flink.table.api.{ Table, TableEnvironment, TableResult }
import org.apache.flink.types.Row

object FlinkRuleLoader {

  /**
   * Load rules from a Flink Table.
   *
   * @param table
   *   A Flink Table (already registered or from a query)
   * @return
   *   List of RuleDefinition
   */
  def fromTable(table: Table): List[RuleDefinition] = {
    // 🔥 Obtém os nomes das colunas diretamente da Table (sempre disponível)
    val fieldNames = table.getSchema.getFieldNames

    val required = Set("field", "check_type")
    val cols     = fieldNames.toSet
    val missing  = required -- cols
    require(missing.isEmpty, s"Missing required columns: ${missing.mkString(", ")}")

    val result: TableResult = table.execute()
    val rows                = result.collect().asScala.toList

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
    // 🔥 Usa sqlQuery para selecionar a coluna (não depende da bridge)
    val tableName           = table.toString
    val sql                 = s"SELECT `$column` FROM $tableName"
    val selected            = tableEnv.sqlQuery(sql)
    val result: TableResult = selected.execute()
    val rows                = result.collect().asScala.toList

    rows.map {
      row =>
        val json = Option(row.getField(0)).map(_.toString).getOrElse("")
        RuleLoader.fromJsonString(json).head
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

    val rows = result.collect().asScala.toList

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
