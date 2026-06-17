// spark/src/main/scala/io/galileostd/sumeh/spark/SparkRuleLoader.scala
package io.galileostd.sumeh.spark.config

import io.galileostd.sumeh.config.RuleLoader
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.spark.sql.DataFrame

object SparkRuleLoader {

  /**
   * Load rules from a Spark DataFrame.
   *
   * Required columns: field, check_type Optional: value, threshold, execute, level, category Extra columns → metadata
   *
   * Example: val df = spark.read.table("dq_rules") val rules = SparkRuleLoader.fromDataFrame(df) val report =
   * SparkValidator.validate(data, rules)
   */
  def fromDataFrame(df: DataFrame): List[RuleDefinition] = {
    val required = Set("field", "check_type")
    val cols     = df.columns.toSet
    val missing  = required -- cols
    require(missing.isEmpty, s"Missing required columns: ${missing.mkString(", ")}")

    df.collect()
      .map {
        row =>
          val map = df.columns.map(col => col -> Option(row.getAs[Any](col)).map(_.toString).getOrElse("")).toMap
          RuleDefinition.fromMap(map)
      }
      .toList
  }

  /**
   * Load rules from a Spark DataFrame that has a single JSON column. Each row is a rule object.
   *
   * Example: val df = spark.read.table("dq_rules_json") val rules = SparkRuleLoader.fromJsonColumn(df, "config")
   */
  def fromJsonColumn(df: DataFrame, column: String = "config"): List[RuleDefinition] = {
    require(df.columns.contains(column), s"Column '$column' not found")

    df.select(column)
      .collect()
      .map {
        row =>
          val json = Option(row.getString(0)).getOrElse("")
          RuleLoader.fromJsonString(json).head
      }
      .toList
  }
}
