// spark/src/main/scala/io/galileostd/sumeh/spark/SparkRuleLoader.scala
package io.galileostd.sumeh.spark.config

import io.galileostd.sumeh.config.RuleLoader
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.spark.sql.DataFrame

/**
 * Loads RuleDefinitions from Spark DataFrames.
 *
 * Required columns: field, check_type. Optional: value, threshold, execute, level, category. Extra columns are
 * preserved as metadata.
 */
object SparkRuleLoader {

  /**
   * Loads rules from a Spark DataFrame.
   *
   * Args: df: DataFrame with columns field, check_type (and optionally the others).
   *
   * Returns: List of RuleDefinition.
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
   * Loads rules from a Spark DataFrame that has a single JSON column; each row is a rule object.
   *
   * Args: df: The DataFrame. column: The column name containing JSON (default "config").
   *
   * Returns: List of RuleDefinition.
   */
  def fromJsonColumn(df: DataFrame, column: String = "config"): List[RuleDefinition] = {
    require(df.columns.contains(column), s"Column '$column' not found")

    df.select(column)
      .collect()
      .flatMap {
        row =>
          val json = Option(row.getString(0)).getOrElse("")
          RuleLoader.fromJsonString(json)
      }
      .toList
  }
}
