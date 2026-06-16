package io.galileostd.sumeh.spark

import io.galileostd.sumeh.engine.Splittable
import org.apache.spark.sql.{ functions => F, DataFrame }

/**
 * Wrapper around a Spark DataFrame with a _dq_errors column. Provides split() to separate good from bad rows.
 */
class ValidatedSparkDataFrame(private val df: DataFrame) {

  def splitByErrors(errorColumn: String = "_dq_errors"): (DataFrame, DataFrame) = {
    require(df.columns.contains(errorColumn), s"Column '$errorColumn' not found")
    val hasErrors = F.size(F.col(errorColumn)) > 0
    val good      = df.filter(!hasErrors).drop(errorColumn)
    val bad       = df.filter(hasErrors)
    (good, bad)
  }

  def toNative: DataFrame = df

  def columns: Array[String]  = df.columns
  def count(): Long           = df.count()
  def show(n: Int = 20): Unit = df.show(n)
}

object ValidatedSparkDataFrame {
  implicit val splittable: Splittable[ValidatedSparkDataFrame] =
    new Splittable[ValidatedSparkDataFrame] {
      def split(df: ValidatedSparkDataFrame): (ValidatedSparkDataFrame, ValidatedSparkDataFrame) = {
        val (good, bad) = df.splitByErrors()
        (new ValidatedSparkDataFrame(good), new ValidatedSparkDataFrame(bad))
      }
    }
}
