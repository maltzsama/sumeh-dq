package io.galileostd.sumeh.spark

import io.galileostd.sumeh.engine.Splittable
import org.apache.spark.sql.{ functions => F, DataFrame }

/**
 * Wrapper around a Spark DataFrame that carries a `_dq_errors` column.
 *
 * Produced by [[io.galileostd.sumeh.spark.SparkValidator]]. Bad rows carry a non-empty `_dq_errors` struct; use
 * `splitByErrors` (or the implicit `Splittable`) to separate good from bad in one pass.
 *
 * Args: df: The validated DataFrame, with the `_dq_errors` column.
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
   * Args: errorColumn: Name of the errors column (default `"_dq_errors"`).
   *
   * Returns: A `(good, bad)` tuple of DataFrames.
   *
   * Throws: IllegalArgumentException when the error column is absent from the DataFrame.
   */
  def splitByErrors(errorColumn: String = "_dq_errors"): (DataFrame, DataFrame) = {
    require(df.columns.contains(errorColumn), s"Column '$errorColumn' not found")
    val hasErrors = F.size(F.col(errorColumn)) > 0
    val good      = df.filter(!hasErrors).drop(errorColumn)
    val bad       = df.filter(hasErrors)
    (good, bad)
  }

  /**
   * The underlying Spark DataFrame.
   *
   * Returns: The raw DataFrame, including the `_dq_errors` column.
   */
  def toNative: DataFrame = df

  /**
   * Column names of the validated DataFrame.
   *
   * Returns: The underlying DataFrame's columns.
   */
  def columns: Array[String] = df.columns

  /**
   * Row count of the validated DataFrame.
   *
   * Returns: The underlying DataFrame's row count.
   */
  def count(): Long = df.count()

  /**
   * Pretty-prints the first `n` rows.
   *
   * Args: n: Number of rows to print (default 20).
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
   * Returns: A `(good, bad)` pair of re-wrapped validated DataFrames.
   */
  implicit val splittable: Splittable[ValidatedSparkDataFrame] =
    new Splittable[ValidatedSparkDataFrame] {

      /**
       * Splits via [[splitByErrors]] and re-wraps both sides.
       *
       * Returns: A `(good, bad)` pair of validated wrappers.
       */
      def split(df: ValidatedSparkDataFrame): (ValidatedSparkDataFrame, ValidatedSparkDataFrame) = {
        val (good, bad) = df.splitByErrors()
        (new ValidatedSparkDataFrame(good), new ValidatedSparkDataFrame(bad))
      }
    }
}
