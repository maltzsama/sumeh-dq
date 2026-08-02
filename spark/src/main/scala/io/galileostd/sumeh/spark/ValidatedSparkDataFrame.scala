package io.galileostd.sumeh.spark

import io.galileostd.sumeh.engine.Splittable
import org.apache.spark.sql.{ functions => F, DataFrame }

/**
 * DataFrame annotated with quality columns by [[io.galileostd.sumeh.spark.SparkValidator]].
 *
 *   - `_dq_errors`: `array<struct<rule_id, check_type, field, category, expected, actual, message, timestamp>>`. Empty
 *     for rows with no violation. The `rule_id` field matches the same field in
 *     [[io.galileostd.sumeh.validation.ValidationReport.summary]], so a failing row can be correlated back to the
 *     validation that flagged it. Field names and order match the Python implementation.
 *   - `_dq_skipped`: a string with `checkType:reason` entries for every skipped rule, separated by `|`. This is a
 *     run-level property — the value is the same in every row.
 *
 * In the Flink engine, `_dq_errors` is a string JSON carrying the same fields, serialized as text. Cross-engine sinks
 * must handle the two shapes.
 *
 * Use [[splitByErrors]] (or the implicit [[io.galileostd.sumeh.engine.Splittable]]) to separate good from bad in one
 * pass.
 *
 * @param df The validated DataFrame, with the `_dq_errors` column.
 */
class ValidatedSparkDataFrame(private val df: DataFrame) {

  /**
   * Splits this DataFrame into good and bad rows based on the error column.
   *
   * Good rows have an empty (or null) `_dq_errors`; bad rows have at least one entry. The good side drops the error
   * column (but keeps `_dq_skipped` when present); the bad side keeps both so you can see which rule failed and why.
   *
   * Note: `threshold` only affects the status of each [[io.galileostd.sumeh.validation.ValidationResult]]. Rows that
   * violate a rule are always marked in `_dq_errors`, even when the rule passes the threshold. A report with pass rate
   * 1.0 can still have rows in the `bad` DataFrame.
   *
   * @param errorColumn Name of the errors column (default `"_dq_errors"`).
   * @return A `(good, bad)` tuple of DataFrames.
   * @throws java.lang.IllegalArgumentException when the error column is absent from the DataFrame.
   */
  def splitByErrors(errorColumn: String = "_dq_errors"): (DataFrame, DataFrame) = {
    require(df.columns.contains(errorColumn), s"Column '$errorColumn' not found")
    val hasErrors = F.coalesce(F.size(F.col(errorColumn)), F.lit(0)) > 0
    val good      = df.filter(!hasErrors).drop(errorColumn)
    val bad       = df.filter(hasErrors)
    (good, bad)
  }

  /**
   * The underlying Spark DataFrame.
   *
   * @return the annotated DataFrame, with `_dq_errors` and optionally `_dq_skipped`
   */
  def toNative: DataFrame = df

  /**
   * Column names of the validated DataFrame.
   *
   * @return the column names of the validated DataFrame
   */
  def columns: Array[String] = df.columns

  /**
   * Row count of the validated DataFrame.
   *
   * @return the number of rows; triggers a Spark job
   */
  def count(): Long = df.count()

  /**
   * Pretty-prints the first `n` rows.
   *
   * @param n Number of rows to print (default 20).
   */
  def show(n: Int = 20): Unit = df.show(n)
}

/**
 * Companion providing the engine `Splittable` instance.
 */
object ValidatedSparkDataFrame {

  /**
   * Implicit `Splittable` instance so `report.split` works on the validated wrapper.
   *
   * @return a pair of [[ValidatedSparkDataFrame]]s — good and bad, ready for further processing
   */
  implicit val splittable: Splittable[ValidatedSparkDataFrame] =
    new Splittable[ValidatedSparkDataFrame] {

      /**
       * Splits via [[splitByErrors]] and re-wraps both sides.
       *
       * @return a `(good, bad)` pair of [[ValidatedSparkDataFrame]]s, both retaining the error column
       */
      def split(df: ValidatedSparkDataFrame): (ValidatedSparkDataFrame, ValidatedSparkDataFrame) = {
        val (good, bad) = df.splitByErrors()
        (new ValidatedSparkDataFrame(good), new ValidatedSparkDataFrame(bad))
      }
    }
}
