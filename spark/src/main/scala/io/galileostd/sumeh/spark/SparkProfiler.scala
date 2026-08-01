package io.galileostd.sumeh.spark

import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.spark.registry.SparkRegistry
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
import org.apache.spark.sql.DataFrame

/**
 * Column-level statistics for a Spark DataFrame.
 *
 * Mirrors the Python `profile(df)` output: for every column it measures completeness and cardinality; for numeric
 * columns it also measures min/max/mean/std/sum. The profiler calls the engine analyzers directly — it wants metrics,
 * not PASS/FAIL verdicts — so no constraint or `_dq_errors` annotation is involved.
 */
object SparkProfiler {

  /**
   * Statistics for a single column.
   *
   * Args: `type`: Canonical column type. nullable: Whether the column allows nulls. rowCount: Total rows profiled.
   * completeness: Fraction of non-null values in `[0.0, 1.0]`. distinctCount: Number of distinct values. nullCount:
   * Estimated number of nulls (`round(rowCount * (1 - completeness))`). uniqueness: `distinctCount / rowCount`. min:
   * Numeric minimum (None for non-numeric columns). max: Numeric maximum. mean: Numeric mean. stdDev: Numeric standard
   * deviation. sum: Numeric sum.
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
     * Returns: A serializable map with snake_case keys; numeric stats are `null` when absent.
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
   * Args: tableStats: Run-level stats (`total_rows`, `columns_count`, `execution_time_ms`). columnProfiles: Column name
   * → [[ColumnProfile]].
   */
  final case class ProfileReport(
      tableStats: Map[String, Any],
      columnProfiles: Map[String, ColumnProfile]
  ) {

    /**
     * Flat map form of the report, with column profiles flattened to maps.
     *
     * Returns: A map shaped `{ "table_stats": {...}, "column_profiles": { col -> {...} } }`.
     */
    def toMap: Map[String, Any] = Map(
      "table_stats"     -> tableStats,
      "column_profiles" -> columnProfiles.map { case (k, v) => k -> v.toMap }
    )

    /**
     * JSON payload for dashboards / metrics endpoints.
     *
     * Returns: The report as a JSON string.
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
   * Computes the row count once and then, for every column, invokes the engine analyzers directly (`is_complete` and
   * `has_cardinality` for all columns; `has_min`/`has_max`/`has_mean`/`has_std`/`has_sum` for numeric ones) to read
   * their metric values. No constraint or report is involved — the profiler wants numbers, not verdicts.
   *
   * Args: df: The DataFrame to profile. sampleFraction: Optional fraction in `(0.0, 1.0)` to sample (with a fixed seed)
   * before profiling.
   *
   * Returns: A [[ProfileReport]] with table stats and per-column profiles.
   */
  def profile(df: DataFrame, sampleFraction: Option[Double] = None): ProfileReport = {
    val target = sampleFraction match {
      case Some(f) if f > 0.0 && f < 1.0 => df.sample(f, seed = 42L)
      case _                             => df
    }

    val fields    = target.schema.fields
    val totalRows = target.count()
    val startTime = System.currentTimeMillis()

    val profiles = fields.map(f => f.name -> buildProfile(f, target, totalRows)).toMap

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
   * Args: f: The struct field.
   *
   * Returns: `true` for byte/short/int/long/float/double/decimal columns.
   */
  private def isNumeric(f: StructField): Boolean =
    numericTypes.contains(f.dataType) || f.dataType.isInstanceOf[DecimalType]

  /**
   * Assembles a [[ColumnProfile]] for one column by calling the analyzers directly.
   *
   * Completeness comes from `is_complete`'s metric, cardinality from `has_cardinality`'s; numeric columns additionally
   * read min/max/mean/std/sum. An analyzer that rejects the column (e.g. `has_min` on a string column) yields `None`
   * for that stat instead of failing the whole profile.
   *
   * Args: field: The schema field. df: The DataFrame to measure. totalRows: Total rows (already counted).
   *
   * Returns: The column profile.
   */
  private def buildProfile(
      field: StructField,
      df: DataFrame,
      totalRows: Long
  ): ColumnProfile = {
    def metricValue(checkType: String): Option[Double] = {
      val rule = RuleDefinition.validated(Left(field.name), checkType)
      try Some(SparkRegistry.getAnalyzer(checkType).analyze(df, rule).value)
      catch { case _: IllegalArgumentException => None }
    }

    val completeness = metricValue("is_complete").getOrElse(1.0)
    val distinct     = metricValue("has_cardinality").getOrElse(0.0)
    val nullCount    = math.round(totalRows * (1.0 - completeness))
    val uniqueness   = if (totalRows > 0) distinct / totalRows else 0.0

    val (min, max, mean, stdDev, sum) =
      if (isNumeric(field))
        (
          metricValue("has_min"),
          metricValue("has_max"),
          metricValue("has_mean"),
          metricValue("has_std"),
          metricValue("has_sum")
        )
      else (None, None, None, None, None)

    ColumnProfile(
      `type` = field.dataType.typeName,
      nullable = field.nullable,
      rowCount = totalRows,
      completeness = completeness,
      distinctCount = distinct.toLong,
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
