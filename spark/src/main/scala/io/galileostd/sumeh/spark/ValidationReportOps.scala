package io.galileostd.sumeh.spark

import io.galileostd.sumeh.validation.ValidationReport
import org.apache.spark.sql.{ DataFrame, Row, SparkSession }
import org.apache.spark.sql.types._

/**
 * Materializes a [[ValidationReport]] as a Spark [[DataFrame]] for persistence.
 *
 * The report lives in the core module, which has no Spark dependency, so the canonical schema and the conversion stay
 * here in the engine. Downstream tables and dashboards depend on [[ValidationReportOps.schema]] — treat it as a data
 * contract.
 */
object ValidationReportOps {

  /**
   * Canonical schema of the metrics table produced by `toDataFrame`.
   *
   * Downstream tables and dashboards depend on it — treat it as a data contract.
   */
  val schema: StructType = StructType(
    Seq(
      StructField("run_id", StringType, nullable = false),
      StructField("run_timestamp", TimestampType, nullable = false),
      StructField("engine", StringType, nullable = false),
      StructField("total_rows", LongType, nullable = false),
      StructField("execution_time_ms", DoubleType, nullable = false),
      StructField("result_id", StringType, nullable = false),
      StructField("check_type", StringType, nullable = false),
      StructField("field", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("level", StringType, nullable = true),
      StructField("status", StringType, nullable = false),
      StructField("pass_rate", DoubleType, nullable = true),
      StructField("expected", DoubleType, nullable = true),
      StructField("actual", DoubleType, nullable = true),
      StructField("fail_count", LongType, nullable = true),
      StructField("message", StringType, nullable = true)
    )
  )

  implicit class ValidationReportDfOps[DF](private val report: ValidationReport[DF]) extends AnyVal {

    /**
     * This report as a flat DataFrame: one row per validation result, with the run-level fields denormalized onto every
     * row.
     *
     * Ready to append to a metrics table — partition by a date derived from `run_timestamp` for a quality time series
     * per rule.
     *
     * @param spark session used to build the DataFrame
     * @return a DataFrame with `ValidationReportOps.schema`; empty (never null) when the report has no results
     */
    def toDataFrame(
        implicit spark: SparkSession
    ): DataFrame = {
      val ts = java.sql.Timestamp.valueOf(report.timestamp)
      val rows = report.results.map {
        r =>
          Row(
            report.runId,
            ts,
            report.engine,
            report.totalRows,
            report.executionTimeMs,
            r.id,
            r.checkType,
            r.fieldName,
            r.category,
            r.level.toString,
            r.status.toString,
            r.passRate.map(java.lang.Double.valueOf).orNull,
            r.expectedValue.map(java.lang.Double.valueOf).orNull,
            r.actualValue.map(java.lang.Double.valueOf).orNull,
            r.failCount.map(java.lang.Long.valueOf).orNull,
            r.message.orNull
          )
      }
      spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    }
  }
}
