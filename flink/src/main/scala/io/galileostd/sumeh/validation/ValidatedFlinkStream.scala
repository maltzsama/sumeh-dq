// flink/src/main/scala/io/galileostd/sumeh/flink/validation/ValidatedFlinkStream.scala
package io.galileostd.sumeh.flink.validation

import org.apache.flink.streaming.api.datastream.{ DataStream, SingleOutputStreamOperator }
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag

/**
 * Wrapper around a Flink `DataStream[Row]` that carries `_dq_errors` / `_dq_skipped` fields.
 *
 * Produced by [[io.galileostd.sumeh.flink.FlinkValidator]]. Provides [[split]] via side outputs — zero reprocessing,
 * zero shuffle.
 *
 * Args: stream: The validated single-output stream (rows carry `_dq_errors` / `_dq_skipped` fields). errorTag:
 * Side-output tag for bad rows. goodTag: Side-output tag for good rows.
 */
class ValidatedFlinkStream(
    private val stream: SingleOutputStreamOperator[Row],
    private val errorTag: OutputTag[Row],
    val goodTag: OutputTag[Row]
) {

  /**
   * Splits the stream into good and bad rows via side outputs.
   *
   * Good rows are those with no `_dq_errors` entries; bad rows have at least one.
   *
   * Returns: A `(good, bad)` tuple of `DataStream[Row]`.
   */
  def split(): (DataStream[Row], DataStream[Row]) = {
    val good = stream.getSideOutput(goodTag)
    val bad  = stream.getSideOutput(errorTag)
    (good, bad)
  }

  /**
   * The underlying single-output stream.
   *
   * Returns: The enriched `SingleOutputStreamOperator[Row]`.
   */
  def toNative: SingleOutputStreamOperator[Row] = stream
}
