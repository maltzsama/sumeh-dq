package io.galileostd.sumeh.spark

import org.apache.spark.sql.{ functions => F, DataFrame }
import org.apache.spark.sql.types.{
  ByteType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  ShortType,
  StructField
}

/**
 * Column-level statistics for a Spark DataFrame.
 *
 * Mirrors the Python `profile(df)` output: for every column it measures completeness and cardinality; for numeric
 * columns it also measures min/max/mean/std/sum. All statistics are computed in a single aggregation pass, so the
 * number of Spark jobs is constant regardless of how many columns the DataFrame has.
 */
object SparkProfiler {

  /**
   * Statistics for a single column.
   *
   * @param `type` Canonical column type.
   * @param nullable Whether the column allows nulls.
   * @param rowCount Total rows profiled.
   * @param completeness Fraction of non-null values in `[0.0, 1.0]`.
   * @param distinctCount Number of distinct values.
   * @param nullCount Estimated number of nulls (`round(rowCount * (1 - completeness))`).
   * @param uniqueness `distinctCount / rowCount`.
   * @param min Numeric minimum (None for non-numeric columns).
   * @param max Numeric maximum.
   * @param mean Numeric mean.
   * @param stdDev Numeric standard deviation.
   * @param sum Numeric sum.
   */
  final case class ColumnProfile(
      `type`: String,
      nullable: Boolean,
      rowCount: Long,
      completeness: Double,
      distinctCount: Long,
      nullCount: Long,
      uniqueness: Double,
      min: Option[Double] = None,
      max: Option[Double] = None,
      mean: Option[Double] = None,
      stdDev: Option[Double] = None,
      sum: Option[Double] = None
  ) {

    /**
     * Flat map form of the profile.
     *
     * @return A serializable map with snake_case keys; numeric stats are `null` when absent.
     */
    def toMap: Map[String, Any] = Map(
      "type"           -> `type`,
      "nullable"       -> nullable,
      "row_count"      -> rowCount,
      "completeness"   -> completeness,
      "distinct_count" -> distinctCount,
      "null_count"     -> nullCount,
      "uniqueness"     -> uniqueness,
      "min"            -> min.orNull,
      "max"            -> max.orNull,
      "mean"           -> mean.orNull,
      "std_dev"        -> stdDev.orNull,
      "sum"            -> sum.orNull
    )
  }

  /**
   * Column-level profile for a full DataFrame.
   *
   * @param tableStats Run-level stats (`total_rows`, `columns_count`, `execution_time_ms`).
   * @param columnProfiles Column name → [[ColumnProfile]].
   */
  final case class ProfileReport(
      tableStats: Map[String, Any],
      columnProfiles: Map[String, ColumnProfile]
  ) {

    /**
     * Flat map form of the report, with column profiles flattened to maps.
     *
     * @return A map shaped `{ "table_stats": {...}, "column_profiles": { col -> {...} } }`.
     */
    def toMap: Map[String, Any] = Map(
      "table_stats"     -> tableStats,
      "column_profiles" -> columnProfiles.map { case (k, v) => k -> v.toMap }
    )

    /**
     * JSON payload for dashboards / metrics endpoints.
     *
     * @return The report as a JSON string.
     */
    def toJson: String = {
      def toValue(v: Any): ujson.Value = v match {
        case i: Int     => ujson.Num(i)
        case l: Long    => ujson.Num(l.toDouble)
        case d: Double  => ujson.Num(d)
        case f: Float   => ujson.Num(f.toDouble)
        case b: Boolean => ujson.Bool(b)
        case null       => ujson.Null
        case other      => ujson.Str(other.toString)
      }
      ujson.write(
        ujson.Obj(
          "table_stats" -> ujson.Obj.from(tableStats.map { case (k, v) => k -> toValue(v) }.toSeq),
          "column_profiles" -> ujson.Obj.from(
            columnProfiles.map {
              case (k, v) => k -> ujson.Obj.from(v.toMap.map { case (k2, v2) => k2 -> toValue(v2) }.toSeq)
            }.toSeq
          )
        )
      )
    }
  }

  /**
   * Spark types treated as numeric for profiling purposes.
   */
  private val numericTypes: Set[org.apache.spark.sql.types.DataType] =
    Set(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)

  /**
   * Profiles a DataFrame.
   *
   * Runs a single aggregation computing every statistic for every column at once, so the number of Spark jobs does not
   * grow with the column count.
   *
   * @param df The DataFrame to profile.
   * @param sampleFraction Optional fraction in `(0.0, 1.0)` to sample (with a fixed seed) before profiling.
   * @return A [[ProfileReport]] with table stats and per-column profiles.
   */
  def profile(df: DataFrame, sampleFraction: Option[Double] = None): ProfileReport = {
    val target = sampleFraction match {
      case Some(f) if f > 0.0 && f < 1.0 => df.sample(f, seed = 42L)
      case _                             => df
    }

    val fields    = target.schema.fields
    val startTime = System.currentTimeMillis()

    // One aggregation column per (field, statistic). The total-count column is the first
    // element, so the recorded index is the exact position in the result row and column
    // names never collide with weird `__` suffixes.
    val index   = scala.collection.mutable.Map[(String, String), Int]()
    val aggCols = scala.collection.mutable.ArrayBuffer[org.apache.spark.sql.Column](F.count(F.lit(1)).alias("__total"))

    fields.foreach {
      f =>
        index((f.name, "nulls")) = aggCols.size
        aggCols += F.sum(F.when(F.col(f.name).isNull, 1L).otherwise(0L))

        index((f.name, "distinct")) = aggCols.size
        aggCols += F.countDistinct(F.col(f.name))

        if (isNumeric(f)) {
          index((f.name, "min")) = aggCols.size
          aggCols += F.min(F.col(f.name)).cast(DoubleType)
          index((f.name, "max")) = aggCols.size
          aggCols += F.max(F.col(f.name)).cast(DoubleType)
          index((f.name, "mean")) = aggCols.size
          aggCols += F.mean(F.col(f.name)).cast(DoubleType)
          index((f.name, "std")) = aggCols.size
          aggCols += F.stddev(F.col(f.name)).cast(DoubleType)
          index((f.name, "sum")) = aggCols.size
          aggCols += F.sum(F.col(f.name)).cast(DoubleType)
        }
    }

    val row       = target.agg(aggCols.head, aggCols.tail.toSeq: _*).collect()(0)
    val totalRows = row.getAs[Long](0)

    val profiles = fields.map(f => f.name -> buildProfile(f, totalRows, row, index)).toMap

    ProfileReport(
      tableStats = Map(
        "total_rows"        -> totalRows,
        "columns_count"     -> fields.length.toLong,
        "execution_time_ms" -> (System.currentTimeMillis() - startTime).toDouble
      ),
      columnProfiles = profiles
    )
  }

  /**
   * Whether a column type is numeric (or decimal) and gets the full numeric statistics.
   *
   * @param f The struct field.
   * @return `true` for byte/short/int/long/float/double/decimal columns.
   */
  private def isNumeric(f: StructField): Boolean =
    numericTypes.contains(f.dataType) || f.dataType.isInstanceOf[DecimalType]

  /**
   * Reads a long statistic from the aggregation row.
   *
   * @param field The column name.
   * @param stat The statistic key.
   * @param row The result row.
   * @param index The (field, stat) → position map.
   * @return The long value.
   */
  private def statLong(
      field: String,
      stat: String,
      row: org.apache.spark.sql.Row,
      index: scala.collection.Map[(String, String), Int]
  ): Long = {
    val i = index((field, stat))
    if (row.isNullAt(i)) 0L else row.getLong(i)
  }

  /**
   * Reads an optional double statistic, returning `None` when the aggregation was null.
   *
   * `min`/`max`/`mean`/`stddev`/`sum` are null on an empty or all-null column. Never call `getAs[Double]` on those
   * directly — unboxing turns null into `0.0` silently.
   *
   * @param field The column name.
   * @param stat The statistic key.
   * @param row The result row.
   * @param index The (field, stat) → position map.
   * @return The double value, or `None` when null.
   */
  private def statOpt(
      field: String,
      stat: String,
      row: org.apache.spark.sql.Row,
      index: scala.collection.Map[(String, String), Int]
  ): Option[Double] = {
    val i = index((field, stat))
    if (row.isNullAt(i)) None else Some(row.getAs[Double](i))
  }

  /**
   * Assembles a [[ColumnProfile]] from the single-pass aggregation row.
   *
   * @param field The schema field.
   * @param totalRows Total rows (from the aggregation).
   * @param row The result row.
   * @param index The (field, stat) → position map.
   * @return The column profile.
   */
  private def buildProfile(
      field: StructField,
      totalRows: Long,
      row: org.apache.spark.sql.Row,
      index: scala.collection.Map[(String, String), Int]
  ): ColumnProfile = {
    val nullCount    = statLong(field.name, "nulls", row, index)
    val completeness = if (totalRows > 0) (totalRows - nullCount).toDouble / totalRows else 1.0
    val distinct     = statLong(field.name, "distinct", row, index)
    val uniqueness   = if (totalRows > 0) distinct.toDouble / totalRows else 0.0

    val (min, max, mean, stdDev, sum) =
      if (isNumeric(field))
        (
          statOpt(field.name, "min", row, index),
          statOpt(field.name, "max", row, index),
          statOpt(field.name, "mean", row, index),
          statOpt(field.name, "std", row, index),
          statOpt(field.name, "sum", row, index)
        )
      else (None, None, None, None, None)

    ColumnProfile(
      `type` = field.dataType.typeName,
      nullable = field.nullable,
      rowCount = totalRows,
      completeness = completeness,
      distinctCount = distinct,
      nullCount = nullCount,
      uniqueness = uniqueness,
      min = min,
      max = max,
      mean = mean,
      stdDev = stdDev,
      sum = sum
    )
  }
}
