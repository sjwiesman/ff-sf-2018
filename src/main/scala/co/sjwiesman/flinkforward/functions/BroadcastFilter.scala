package co.sjwiesman.flinkforward.functions

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * A two stream operator that filter's out all
  * words from the first input that have been seen
  * by the second. It is called a broadcast filter
  * because it is assumed the second stream use's
  * a broadcast partitioning (although this is not
  * statically verified).
  *
  * Because we expect the full set of filter words to
  * easily fit in memory, it is more efficient to
  * broadcast them to each task manager as to
  * avoid network shuffle on the main stream.
  */
class BroadcastFilter extends RichCoFlatMapFunction[String, String, String] with CheckpointedFunction {
  val filters = mutable.Set.empty[String]

  var state: ListState[String] = _

  override def flatMap1(value: String, out: Collector[String]): Unit = {
    if (!filters.contains(value)) {
      out.collect(value)
    }
  }

  override def flatMap2(value: String, out: Collector[String]): Unit = {
    filters += value
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.clear()

    /**
      * Because
      * 1) We have broadcast the full dataset to each operator
      * 2) We are using a union state repartitioning strategy on restore
      *
      * This only a single subtask needs to write out it's state.
      */
    if (getRuntimeContext.getIndexOfThisSubtask == 0) {
      for (filter ← filters) {
        state.add(filter)
      }
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    state = context.getOperatorStateStore.getUnionListState(
      new ListStateDescriptor[String]("filters", BasicTypeInfo.STRING_TYPE_INFO)
    )

    if (context.isRestored) {
      filters.clear()
      val iterable = state.get()
      val iterator = if (iterable != null) iterable.iterator() else null

      while (iterator != null && iterator.hasNext) {
        filters += iterator.next()
      }
    }
  }
}
