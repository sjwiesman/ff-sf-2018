package co.sjwiesman.flinkforward.source

import java.util

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * A simple source to read a file from the resources folder
  * on the class path, providing exactly once read
  * guarantees.
  * @param path - the path to some resource file
  */
class ResourceToStream(path: String) extends RichSourceFunction[String] with ListCheckpointed[Integer] {
  var numLines: Int = 0
  @volatile var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val resource = getClass.getResourceAsStream(path)
    val stream = scala.io.Source.fromInputStream(resource)

    val lines = stream.getLines()
    for (_ ← 0 until numLines) {
      lines.next()
    }

    while (isRunning && lines.hasNext) {
      ctx.getCheckpointLock.synchronized {
        ctx.collect(lines.next())
        numLines += 1
      }
    }

    stream.close()
  }

  override def restoreState(state: util.List[Integer]): Unit = {
    assert(state.size() == 1, s"ResourceToStream state must always be exactly one element but ${state.size()} elements found")
    numLines = state.get(0)
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Integer] = {
    util.Arrays.asList(numLines)
  }
}