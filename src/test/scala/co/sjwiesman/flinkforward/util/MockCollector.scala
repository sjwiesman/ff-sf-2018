package co.sjwiesman.flinkforward.util

import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer

class MockCollector[T] extends Collector[T] {
  var isClosed = false 

  val output = ListBuffer.empty[T]

  def collect(element: T): Unit = {
    if (isClosed) {
      throw new IllegalStateException("Collector cannot collect after it has been closed")
    }

    output += element
  }

  def close(): Unit = {
    isClosed = true
  }
}
