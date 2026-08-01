package io.galileostd.sumeh.flink

import io.galileostd.sumeh.flink.internal.DQProcessFunction
import io.galileostd.sumeh.flink.validation.ValidatedFlinkStream
import io.galileostd.sumeh.rule.RuleDefinition
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
   * Each record is enriched with `_dq_errors` and `_dq_skipped` fields; good records go to the good side output and bad
   * records to the error side output (see `split`).
   *
   * Args: stream: The input `DataStream[Row]`. rules: Rules to apply.
   *
   * Returns: A [[io.galileostd.sumeh.flink.validation.ValidatedFlinkStream]] whose `split()` exposes the `(good, bad)`
   * side outputs.
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
