package io.galileostd.sumeh.spark

import org.apache.spark.sql.functions.{ to_date, try_to_timestamp }
import org.apache.spark.sql.Column

/**
 * ANSI-safe date parsing for Spark.
 *
 * Since Spark 3.5 (the compilation floor) `to_date(col)` throws on unparseable input under ANSI mode, which would kill
 * a streaming query at execution time. `to_date(try_to_timestamp(col))` returns null instead — treating invalid dates
 * as not failing on Spark 3.5 and 4.x alike, since `try_to_timestamp` is available in both.
 */
private[galileostd] object DateExpr {

  /**
   * Converts a column to a date, returning null (not throwing) on unparseable input.
   *
   * ANSI-safe (where `to_date` throws on bad input); preserves null-on-invalid semantics on Spark 3.5+ and 4.x via
   * `try_to_timestamp`.
   *
   * Args: col: The column to convert.
   *
   * Returns: A date column expression.
   */
  def safeToDate(col: Column): Column = to_date(try_to_timestamp(col))
}
