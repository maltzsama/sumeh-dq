package io.galileostd.sumeh.spark

import org.apache.spark.sql.functions.{ to_date, try_to_timestamp }
import org.apache.spark.sql.Column

/**
 * ANSI-safe date parsing for Spark 4 (ANSI mode on by default): `to_date(col)` throws a SparkDateTimeException on
 * unparseable input, which would kill a streaming query at execution time. `to_date(try_to_timestamp(col))` returns
 * null instead — preserving Spark 3.x semantics (invalid dates are treated as not failing) on both 3.5 and 4.x, since
 * `try_to_timestamp` is available in both.
 */
private[galileostd] object DateExpr {
  def safeToDate(col: Column): Column = to_date(try_to_timestamp(col))
}
