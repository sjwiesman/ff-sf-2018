package co.sjwiesman.flinkforward.functions

import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness
import co.sjwiesman.flinkforward.util.MockCollector
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class `2_BroadcastFilterSpec` extends FlatSpec with Matchers {
  it should "filter out blacklisted words" in {
    info("Similarly to the source, we can test the business logic")
    info("of the broadcast filter in isolation from state management.")
    info("Keeping these test's separate will help us track down future errors")
    info("because if this test succeeds but the other fail's we know the issue")
    info("must be with how we are storing state.")

    val filter = new BroadcastFilter
    val out = new MockCollector[String]

    filter.flatMap2("hello", out)
    out.output.size should be (0)

    filter.flatMap1("hello", out)
    out.output.size should be (0)

    filter.flatMap1("world", out)
    out.output.size should be (1)
  }

  it should "restore from state" in {
    info("Unlike the source function, the BroadcastFiler manages")
    info("its state using the lowe level, CheckpointedFunction interface.")
    info("Instead of mocking out the underlying state store ourselves we will turn to")
    info("the operator test harness suite.")

    val initialFilter   = new BroadcastFilter
    val initialOperator = new CoStreamFlatMap(initialFilter)
    val initialTestHarness = new TwoInputStreamOperatorTestHarness(initialOperator)

    initialTestHarness.initializeState(new OperatorStateHandles(0, null, null, null, null))
    initialTestHarness.open()

    initialTestHarness.processElement2(new StreamRecord[String]("hello"))
    initialTestHarness.processElement1(new StreamRecord[String]("hello"))

    initialTestHarness.getOutput.size() should be (0)

    info("These test suites allow us to take a snapshot of our state,")
    val snapshot = initialTestHarness.snapshot(0L, 0L)

    initialTestHarness.close()

    val restoreFilter = new BroadcastFilter
    val restoreOperator = new CoStreamFlatMap(restoreFilter)
    val restoreTestHarness = new TwoInputStreamOperatorTestHarness(restoreOperator)

    info("and then restore a new operator from that snapshot.")
    restoreTestHarness.initializeState(snapshot)
    restoreTestHarness.setup()
    restoreTestHarness.open()

    restoreTestHarness.processElement1(new StreamRecord[String]("hello"))
    restoreTestHarness.getOutput.size() should be (0)

    restoreTestHarness.close()
  }

}
