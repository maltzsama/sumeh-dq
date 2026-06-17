// flink/src/main/scala/io/galileostd/sumeh/flink/validation/ValidatedFlinkStream.scala
package io.galileostd.sumeh.flink.validation

import org.apache.flink.streaming.api.datastream.{ DataStream, SingleOutputStreamOperator }
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag

/**
 * Wrapper around a Flink DataStream[Row] with _dq_errors field. Provides split() via SideOutput — zero reprocessing,
 * zero shuffle.
 */
class ValidatedFlinkStream(
    private val stream: SingleOutputStreamOperator[Row],
    private val errorTag: OutputTag[Row],
    val goodTag: OutputTag[Row]
) {

  def split(): (DataStream[Row], DataStream[Row]) = {
    val good = stream.getSideOutput(goodTag)
    val bad  = stream.getSideOutput(errorTag)
    (good, bad)
  }

  def toNative: SingleOutputStreamOperator[Row] = stream
}
