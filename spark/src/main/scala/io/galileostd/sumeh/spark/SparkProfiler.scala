package io.galileostd.sumeh.spark

import io.galileostd.sumeh.rule.RuleDefinition
import io.galileostd.sumeh.validation.ValidationResult
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
 * Column-level statistics for a Spark DataFrame, computed in a single validation pass.
 *
 * Mirrors the Python `profile(df)` output: for every column it measures completeness and cardinality; for numeric
 * columns it also measures min/max/mean/std/sum. The profiler reuses the existing [[SparkValidator]] analyzers, so no
 * extra scan or UDF is introduced.
 */
object SparkProfiler {

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

  final case class ProfileReport(
      tableStats: Map[String, Any],
      columnProfiles: Map[String, ColumnProfile]
  ) {
    def toMap: Map[String, Any] = Map(
      "table_stats"     -> tableStats,
      "column_profiles" -> columnProfiles.map { case (k, v) => k -> v.toMap }
    )

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

  private val numericTypes: Set[org.apache.spark.sql.types.DataType] =
    Set(ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)

  /**
   * Profile a DataFrame.
   *
   * @param df
   *   DataFrame to profile
   * @param sampleFraction
   *   Optional fraction (0.0–1.0) to sample before profiling
   */
  def profile(df: DataFrame, sampleFraction: Option[Double] = None): ProfileReport = {
    val target = sampleFraction match {
      case Some(f) if f > 0.0 && f < 1.0 => df.sample(f, seed = 42L)
      case _                             => df
    }

    val fields = target.schema.fields

    val rules = fields.flatMap {
      f =>
        val base = Seq(
          RuleDefinition.validated(Left(f.name), "is_complete"),
          RuleDefinition.validated(Left(f.name), "has_cardinality")
        )
        if (isNumeric(f)) {
          base ++ Seq(
            RuleDefinition.validated(Left(f.name), "has_min"),
            RuleDefinition.validated(Left(f.name), "has_max"),
            RuleDefinition.validated(Left(f.name), "has_mean"),
            RuleDefinition.validated(Left(f.name), "has_std"),
            RuleDefinition.validated(Left(f.name), "has_sum")
          )
        } else base
    }

    val startTime = System.currentTimeMillis()
    val report    = SparkValidator.validate(target, rules)
    val elapsedMs = (System.currentTimeMillis() - startTime).toDouble

    val profiles = fields.map {
      f =>
        val colResults = report.results.filter(_.fieldName == f.name)
        f.name -> buildProfile(f, colResults, report.totalRows)
    }.toMap

    ProfileReport(
      tableStats = Map(
        "total_rows"        -> report.totalRows,
        "columns_count"     -> fields.length.toLong,
        "execution_time_ms" -> elapsedMs
      ),
      columnProfiles = profiles
    )
  }

  private def isNumeric(f: StructField): Boolean =
    numericTypes.contains(f.dataType) || f.dataType.isInstanceOf[DecimalType]

  private def buildProfile(
      field: StructField,
      results: Seq[ValidationResult],
      totalRows: Long
  ): ColumnProfile = {
    val byCheck = results.flatMap(r => r.actualValue.map(r.checkType -> _)).toMap

    val completeness = byCheck.getOrElse("is_complete", 1.0)
    val distinct     = byCheck.getOrElse("has_cardinality", 0.0)
    val nullCount    = math.round(totalRows * (1.0 - completeness))
    val uniqueness   = if (totalRows > 0) distinct / totalRows else 0.0

    ColumnProfile(
      `type` = field.dataType.typeName,
      nullable = field.nullable,
      rowCount = totalRows,
      completeness = completeness,
      distinctCount = distinct.toLong,
      nullCount = nullCount,
      uniqueness = uniqueness,
      min = byCheck.get("has_min"),
      max = byCheck.get("has_max"),
      mean = byCheck.get("has_mean"),
      stdDev = byCheck.get("has_std"),
      sum = byCheck.get("has_sum")
    )
  }
}
