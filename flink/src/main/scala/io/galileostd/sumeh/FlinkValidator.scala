package io.galileostd.sumeh.flink

import io.galileostd.sumeh.flink.internal.DQProcessFunction
import io.galileostd.sumeh.flink.validation.ValidatedFlinkStream
import io.galileostd.sumeh.rule.RuleDefinition
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag

object FlinkValidator {

  def validate(
      stream: DataStream[Row],
      rules: Seq[RuleDefinition]
  ): ValidatedFlinkStream = {

    val rowRules = rules.filter(_.isApplicableForLevel("ROW"))

    val gTag = new OutputTag[Row]("_dq_good") {}
    val eTag = new OutputTag[Row]("_dq_errors") {}

    val processed = stream.process(new DQProcessFunction(rowRules, gTag, eTag))

    new ValidatedFlinkStream(processed, eTag, gTag)
  }
}
