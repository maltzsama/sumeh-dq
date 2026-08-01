package io.galileostd.sumeh.flink

import io.galileostd.sumeh.flink.internal.DQProcessFunction
import io.galileostd.sumeh.flink.validation.ValidatedFlinkStream
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag

/**
 * Flink entry point: validates DataStream[Row] records with side-output bifurcation.
 *
 * Streaming is stateless by design — each record is evaluated independently. Rules that need state (uniqueness,
 * TABLE-level aggregation) or custom SQL are skipped with a reason.
 */
object FlinkValidator {

  /**
   * Validates a stream and returns a ValidatedFlinkStream.
   *
   * Args: stream: The input DataStream[Row]. rules: Rules to apply.
   *
   * Returns: A ValidatedFlinkStream whose split() exposes the (good, bad) side outputs.
   */
  def validate(
      stream: DataStream[Row],
      rules: Seq[RuleDefinition]
  ): ValidatedFlinkStream = {

    val gTag = new OutputTag[Row]("_dq_good") {}
    val eTag = new OutputTag[Row]("_dq_errors") {}

    val processed = stream.process(new DQProcessFunction(rules, gTag, eTag))

    new ValidatedFlinkStream(processed, eTag, gTag)
  }
}
