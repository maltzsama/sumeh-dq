// flink/src/main/scala/io/galileostd/sumeh/flink/validation/ValidatedFlinkStream.scala
package io.galileostd.sumeh.flink.validation

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.{ DataStream, SingleOutputStreamOperator }
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag

/**
 * Wrapper around a Flink `DataStream[Row]` that carries `_dq_errors` / `_dq_skipped` fields.
 *
 * Produced by [[io.galileostd.sumeh.flink.FlinkValidator]]. Provides [[split]] — bad rows come from the error side
 * output, good rows are filtered from the main output. Zero reprocessing, zero shuffle.
 *
 * Args: stream: The validated single-output stream (rows carry `_dq_errors` / `_dq_skipped` fields). errorTag:
 * Side-output tag for bad rows.
 */
class ValidatedFlinkStream(
    private val stream: SingleOutputStreamOperator[Row],
    private val errorTag: OutputTag[Row]
) {

  /**
   * Splits the stream into good and bad rows.
   *
   * Good rows are those with an empty (or null) `_dq_errors` value; bad rows have at least one error and come from the
   * error side output.
   *
   * Returns: A `(good, bad)` tuple of `DataStream[Row]`.
   */
  def split(): (DataStream[Row], DataStream[Row]) = {
    val errorIdx = stream.getType.asInstanceOf[RowTypeInfo].getFieldIndex("_dq_errors")
    val good = stream.filter(new FilterFunction[Row] {
      override def filter(r: Row): Boolean = {
        val v = r.getField(errorIdx)
        v == null || v.toString.isEmpty
      }
    })
    (good, stream.getSideOutput(errorTag))
  }

  /**
   * The underlying single-output stream.
   *
   * Returns: The enriched `SingleOutputStreamOperator[Row]`.
   */
  def toNative: SingleOutputStreamOperator[Row] = stream
}
