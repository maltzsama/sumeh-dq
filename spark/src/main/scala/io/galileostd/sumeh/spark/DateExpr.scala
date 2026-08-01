package io.galileostd.sumeh.spark

import org.apache.spark.sql.functions.{ to_date, try_to_timestamp }
import org.apache.spark.sql.Column

/**
 * ANSI-safe date parsing for Spark 4.
 *
 * In Spark 4 (ANSI mode on by default) `to_date(col)` throws a `SparkDateTimeException` on unparseable input, which
 * would kill a streaming query at execution time. `to_date(try_to_timestamp(col))` returns null instead — preserving
 * Spark 3.x semantics (invalid dates are treated as not failing) on both 3.5 and 4.x, since `try_to_timestamp` is
 * available in both.
 */
private[galileostd] object DateExpr {

  /**
   * Converts a column to a date, returning null (not throwing) on unparseable input.
   *
   * ANSI-safe for Spark 4 (where `to_date` throws on bad input); preserves Spark 3.x semantics on both 3.5 and 4.x via
   * `try_to_timestamp`.
   *
   * Args: col: The column to convert.
   *
   * Returns: A date column expression.
   */
  def safeToDate(col: Column): Column = to_date(try_to_timestamp(col))
}
