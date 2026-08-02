// spark/src/main/scala/io/galileostd/sumeh/spark/SparkRuleLoader.scala
package io.galileostd.sumeh.spark.config

import io.galileostd.sumeh.config.RuleLoader
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.spark.sql.DataFrame

/**
 * Loads `RuleDefinition`s from Spark DataFrames.
 *
 * The standard layout has the columns `field`, `check_type` (required) and optionally `value`, `threshold`, `execute`,
 * `level`, `category`. Any extra columns are preserved as rule metadata.
 */
object SparkRuleLoader {

  /**
   * Loads rules from a Spark DataFrame in the standard column layout.
   *
   * Each row becomes one rule via `RuleDefinition.fromMap`; cell values are stringified so numbers and booleans parse
   * cleanly.
   *
   * @param df DataFrame with `field` and `check_type` columns (and optionally the others).
   * @return a list of rules parsed from the DataFrame rows
   * @throws java.lang.IllegalArgumentException when `field` or `check_type` is missing.
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
   * Loads rules from a DataFrame where each row holds one rule as a JSON object.
   *
   * @param df The DataFrame.
   * @param column The column containing the JSON (default `"config"`).
   * @return the list of rules, flattened from JSON arrays across all rows
   * @throws java.lang.IllegalArgumentException when the column does not exist.
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
