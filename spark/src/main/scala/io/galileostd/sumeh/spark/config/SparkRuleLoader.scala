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
   * Upper bound on the number of rows a rule source may hold.
   *
   * `fromDataFrame`/`fromJsonColumn` materialize every row on the driver, so a table pointed at by mistake instead of a
   * rule table would otherwise pull millions of rows into the driver and OOM it before any check runs. The limit is
   * enforced before the `collect` (via `limit(MaxRules + 1)`), so the failure is a clear `IllegalArgumentException`,
   * not an OOM.
   */
  private val MaxRules = 10000

  /**
   * Loads rules from a Spark DataFrame in the standard column layout.
   *
   * Each row becomes one rule via `RuleDefinition.fromMap`; cell values are stringified so numbers and booleans parse
   * cleanly.
   *
   * @param df DataFrame with `field` and `check_type` columns (and optionally the others).
   * @return a list of rules parsed from the DataFrame rows
   * @throws java.lang.IllegalArgumentException when `field` or `check_type` is missing, or when the source has more
   *                                             than [[MaxRules]] rows.
   */
  def fromDataFrame(df: DataFrame): List[RuleDefinition] = {
    val required = Set("field", "check_type")
    val cols     = df.columns.toSet
    val missing  = required -- cols
    require(missing.isEmpty, s"Missing required columns: ${missing.mkString(", ")}")

    val rows = df.limit(MaxRules + 1).collect()
    require(
      rows.length <= MaxRules,
      s"Rule source has more than $MaxRules rows — this does not look like a rule table. " +
        "fromDataFrame materializes every row on the driver; point it at a rule definition table, not at your data."
    )

    rows.map {
      row =>
        val map = df.columns.map(col => col -> Option(row.getAs[Any](col)).map(_.toString).getOrElse("")).toMap
        RuleDefinition.fromMap(map)
    }.toList
  }

  /**
   * Loads rules from a DataFrame where each row holds one rule as a JSON object.
   *
   * @param df The DataFrame.
   * @param column The column containing the JSON (default `"config"`).
   * @return the list of rules, flattened from JSON arrays across all rows
   * @throws java.lang.IllegalArgumentException when the column does not exist, or when the source has more than
   *                                             [[MaxRules]] rows.
   */
  def fromJsonColumn(df: DataFrame, column: String = "config"): List[RuleDefinition] = {
    require(df.columns.contains(column), s"Column '$column' not found")

    val rows = df.select(column).limit(MaxRules + 1).collect()
    require(
      rows.length <= MaxRules,
      s"Rule source has more than $MaxRules rows — this does not look like a rule table. " +
        "fromJsonColumn materializes every row on the driver; point it at a rule definition table, not at your data."
    )

    rows.flatMap {
      row =>
        val json = Option(row.getString(0)).getOrElse("")
        RuleLoader.fromJsonString(json)
    }.toList
  }
}
