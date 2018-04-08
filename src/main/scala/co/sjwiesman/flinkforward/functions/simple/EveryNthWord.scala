package co.sjwiesman.flinkforward.functions.simple

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.util.Collector

/**
  * A simple function using keyed state
  * that outputs the n'th instance of
  * every words in the stream.
  */
class EveryNthWord(n: Int) extends RichFlatMapFunction[String, String] {
  @transient private lazy val countDesc = new ValueStateDescriptor("count", classOf[Int])

  override def flatMap(value: String, out: Collector[String]): Unit = {
    val countState = getRuntimeContext.getState(countDesc)
    val count = Option(countState.value()).map(_.intValue()).getOrElse(1)

    if (count == n) {
      countState.clear()
      out.collect(value)
    } else {
      countState.update(count + 1)
    }
  }
}








