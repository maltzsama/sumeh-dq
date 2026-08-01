package io.galileostd.sumeh.flink

import io.galileostd.sumeh.flink.internal.DQProcessFunction
import io.galileostd.sumeh.flink.validation.ValidatedFlinkStream
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag

/**
 * Flink entry point: validates `DataStream[Row]` records with side-output bifurcation.
 *
 * Streaming is stateless by design — each record is evaluated independently. Rules that need state (uniqueness,
 * TABLE-level aggregation) or custom SQL are skipped with a reason, never silently passed.
 */
object FlinkValidator {

  /**
   * Validates a stream and returns a [[io.galileostd.sumeh.flink.validation.ValidatedFlinkStream]].
   *
   * Each record is enriched with `_dq_errors` and `_dq_skipped` fields; records with at least one error are also routed
   * to the error side output (see `split`). Field names come from the stream's `RowTypeInfo`, so positional rows from
   * real sources are supported. The output type is declared explicitly, keeping the pipeline on the row type instead of
   * falling back to Kryo.
   *
   * Args: stream: The input `DataStream[Row]`, whose type must be a [[RowTypeInfo]]. rules: Rules to apply.
   *
   * Returns: A [[io.galileostd.sumeh.flink.validation.ValidatedFlinkStream]] whose `split()` exposes the `(good, bad)`
   * streams.
   *
   * Throws: IllegalArgumentException when `stream` does not carry an explicit `RowTypeInfo` (e.g. an inferred
   * `GenericTypeInfo[Row]`).
   */
  def validate(
      stream: DataStream[Row],
      rules: Seq[RuleDefinition]
  ): ValidatedFlinkStream = {

    val inType = stream.getType match {
      case rti: RowTypeInfo => rti
      case other =>
        throw new IllegalArgumentException(
          "FlinkValidator requires a DataStream[Row] with an explicit RowTypeInfo. " +
            s"Received: $other. Add .returns(new RowTypeInfo(types, names)) on your source."
        )
    }

    val fieldNames = inType.getFieldNames // Array[String], in positional order
    val fieldTypes = inType.getFieldTypes // Array[TypeInformation[_]]

    val outType = new RowTypeInfo(
      fieldTypes :+ Types.STRING :+ Types.STRING,
      fieldNames :+ "_dq_errors" :+ "_dq_skipped"
    )

    val errorTag = new OutputTag[Row]("_dq_errors", outType)

    val processed = stream
      .process(new DQProcessFunction(rules, fieldNames, errorTag))
      .returns(outType)

    new ValidatedFlinkStream(processed, errorTag)
  }
}
