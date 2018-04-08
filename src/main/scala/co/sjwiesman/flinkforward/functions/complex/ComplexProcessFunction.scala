package co.sjwiesman.flinkforward.functions.complex

import org.apache.flink.api.common.state.{ReducingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.TimeDomain
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

/**
  * A process function that
  * 1) groups all words of the same key together that occur within 30 seconds of each other[1]
  * 2) when the third instance of a word is observed, it is output downstream immediately
  * 3) when a timer finally goes off:
  *   a) all state is cleared
  *   b) if more than 3 instances of a particular word have been
  *   observed, it is output to a heavy hitter's side output
  *   c) if less than 3 instances of a particular word have been
  *   observed, it is output to an infrequent elements side output
  *
  * Unlike it's stateless counterpart, this object makes no
  * distinction between business logic and state management.
  * This means that one cannot be reasoned about withtout the other.
  *
  * @param few infrequent elements tag
  * @param many heavy hitters tag
  *
  * [1] This is only true if watermarks are pushed downstream every 30 seconds.
  * In general you should use a session window for a more robust implementation of this
  * functionality.
  */
class ComplexProcessFunction(few: OutputTag[String], many: OutputTag[String]) extends ProcessFunction[String, String] {
  @transient private[this] lazy val wordDesc  = new ValueStateDescriptor("word", BasicTypeInfo.STRING_TYPE_INFO)
  @transient private[this] lazy val countDesc = new ReducingStateDescriptor("count", null, BasicTypeInfo.LONG_TYPE_INFO)
  @transient private[this] lazy val timerDesc = new ValueStateDescriptor("timer", BasicTypeInfo.LONG_TYPE_INFO)

  override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
    getRuntimeContext.getState(wordDesc).update(value)
    getRuntimeContext.getReducingState(countDesc).add(1L)

    val count = getRuntimeContext.getReducingState(countDesc).get()

    if (count == 2L) {
      out.collect(value)
    }

    val time = ctx.timerService().currentWatermark() + (30 * 1000L)
    getRuntimeContext.getState(timerDesc).update(time)
    ctx.timerService().registerEventTimeTimer(time)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[String, String]#OnTimerContext, out: Collector[String]): Unit = {
    val timer = getRuntimeContext.getState(timerDesc).value()

    if (timestamp == timer && ctx.timeDomain() == TimeDomain.EVENT_TIME) {
      val count = getRuntimeContext.getReducingState(countDesc).get()
      val word  = getRuntimeContext.getState(wordDesc).value()

      if (count < 2) {
        ctx.output(few, word)
      } else if (count > 3) {
        ctx.output(many, word)
      }

      getRuntimeContext.getReducingState(countDesc).clear()
      getRuntimeContext.getState(wordDesc).clear()
      getRuntimeContext.getState(timerDesc).clear()
    }
  }
}
