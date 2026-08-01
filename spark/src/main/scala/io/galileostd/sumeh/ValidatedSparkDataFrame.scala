package io.galileostd.sumeh.spark

import io.galileostd.sumeh.engine.Splittable
import org.apache.spark.sql.{ functions => F, DataFrame }

/**
 * Wrapper around a Spark DataFrame with a `_dq_errors` column.
 *
 * Provides split() to separate good from bad rows (the Bifurcation Pattern).
 *
 * Args: df: The validated DataFrame (carries the `_dq_errors` column).
 */
class ValidatedSparkDataFrame(private val df: DataFrame) {

  /**
   * Splits this DataFrame into good and bad rows based on the error column.
   *
   * Args: errorColumn: Name of the errors column (default "_dq_errors").
   *
   * Returns: A (good, bad) tuple. Good rows drop the error column; bad rows keep it.
   */
  def splitByErrors(errorColumn: String = "_dq_errors"): (DataFrame, DataFrame) = {
    require(df.columns.contains(errorColumn), s"Column '$errorColumn' not found")
    val hasErrors = F.size(F.col(errorColumn)) > 0
    val good      = df.filter(!hasErrors).drop(errorColumn)
    val bad       = df.filter(hasErrors)
    (good, bad)
  }

  /** The underlying Spark DataFrame. */
  def toNative: DataFrame = df

  /** Column names of the validated DataFrame. */
  def columns: Array[String] = df.columns

  /** Row count of the validated DataFrame. */
  def count(): Long = df.count()

  /** Pretty-print the first `n` rows. */
  def show(n: Int = 20): Unit = df.show(n)
}

/** Companion providing the engine Splittable instance. */
object ValidatedSparkDataFrame {
  implicit val splittable: Splittable[ValidatedSparkDataFrame] =
    new Splittable[ValidatedSparkDataFrame] {
      def split(df: ValidatedSparkDataFrame): (ValidatedSparkDataFrame, ValidatedSparkDataFrame) = {
        val (good, bad) = df.splitByErrors()
        (new ValidatedSparkDataFrame(good), new ValidatedSparkDataFrame(bad))
      }
    }
}
