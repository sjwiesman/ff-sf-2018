package co.sjwiesman.flinkforward.util

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.mutable.ListBuffer

class MockSourceContext[T] extends SourceFunction.SourceContext[T] {
  var isClosed = false

  val output: ListBuffer[T] = ListBuffer.empty[T]
  val lock = new Object

  override def collect(element: T): Unit = {
    if (isClosed) {
      throw new IllegalStateException("source context cannot collected after close")
    }
    output += element
  }

  override def collectWithTimestamp(element: T, timestamp: Long): Unit = {
    if (isClosed) {
      throw new IllegalStateException("source context cannot collected after close")
    }
    output += element
  }

  override def getCheckpointLock: AnyRef = lock

  override def markAsTemporarilyIdle(): Unit = {}

  override def emitWatermark(mark: Watermark): Unit = {}

  override def close(): Unit = {
    isClosed = true
  }
}
