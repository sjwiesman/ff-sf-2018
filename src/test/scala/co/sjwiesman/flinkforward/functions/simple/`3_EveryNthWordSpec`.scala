package co.sjwiesman.flinkforward.functions.simple

import co.sjwiesman.flinkforward.functions.simple.`3_EveryNthWordSpec`.IdentityKeySelector
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.operators.StreamFlatMap
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.{FlatSpec, Matchers}

object `3_EveryNthWordSpec` {
  class IdentityKeySelector[T] extends KeySelector[T, T] {
    override def getKey(value: T): T = value
  }
}

class `3_EveryNthWordSpec` extends FlatSpec with Matchers {

  it should "initialize state the first time it sees a word" in {
    info("The key difference, in terms of testing, between")
    info("operator state and keyed state is that naive uses of keyed")
    info("state intertwine business logic and state management.")
    info("Simply put, we cannot test one without the other.")

    val operator    = new StreamFlatMap(new EveryNthWord(3))
    val testHarness = new KeyedOneInputStreamOperatorTestHarness(operator, new IdentityKeySelector[String], BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processElement("hello", 1L)
    testHarness.numKeyedStateEntries() should be (1)

    testHarness.getOutput.size() should be (0)
  }

  it should "modify state in the middle of a run" in {
    info("Because these test's are based around the operator test suite")
    info("Which as of now (Flink 1.4.0) are not part of the public api,")
    info("these test's are brittle and may break across updates.")

    val operator    = new StreamFlatMap(new EveryNthWord(3))
    val testHarness = new KeyedOneInputStreamOperatorTestHarness(operator, new IdentityKeySelector[String], BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processElement("hello", 1L)
    testHarness.processElement("hello", 1L)

    testHarness.numKeyedStateEntries() should be (1)

    testHarness.getOutput.size() should be (0)
  }

  it should "modify output the word and reset state when count equals N" in {
    info("It is also not possible to test certain state's without")
    info("manually pushing the right set of elements through the operator")
    info("in the correct order.")

    val operator    = new StreamFlatMap(new EveryNthWord(3))
    val testHarness = new KeyedOneInputStreamOperatorTestHarness(operator, new IdentityKeySelector[String], BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    testHarness.processElement("hello", 1L)
    testHarness.processElement("hello", 1L)
    testHarness.processElement("hello", 1L)

    testHarness.numKeyedStateEntries() should be (0)

    testHarness.getOutput.size() should be (1)
  }
}
