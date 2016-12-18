package net.wrap_trap.goju

import java.io.File

import akka.actor.{Actor, Props, ActorSystem}
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.{Element, KeyValue}
import org.scalatest._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class GojuSpec extends TestKit(ActorSystem("goju"))
  with FlatSpecLike
  with ShouldMatchers
  with StopSystemAfterAll
  with BeforeAndAfter
  with PlainRpcClient {
  val log = Logger(LoggerFactory.getLogger(this.getClass))
  before {
    TestHelper.deleteDirectory(new File("test-data"))
  }

  "Open, put and get" should "return the value" in {
    import org.scalatest.OptionValues._

    val goju = Goju.open("test-data")
    val key = Utils.toBytes("data")
    val value = Utils.to8Bytes(77)
    goju.put(key, value)
    val optionValue = goju.get(key)
    optionValue should be(defined)
    optionValue.value should be(value)
    goju.destroy()
  }

  "Put multi-bytes strings and get" should "return the value" in {
    import org.scalatest.OptionValues._

    val goju = Goju.open("test-data")
    val key = Utils.toBytes("テスト")
    val value = Utils.toBytes("太郎")
    goju.put(key, value)
    val optionValue = goju.get(key)
    optionValue should be(defined)
    optionValue.value should be(value)
    goju.destroy()
  }

  "Put one that will be expired after 2 seconds and get" should "return None" in {

    val goju = Goju.open("test-data")
    val key = Utils.toBytes("expire-test")
    val value = Utils.toBytes("hoge")
    goju.put(key, value, 2)
    Thread.sleep(3000)
    goju.get(key) should not be(defined)
    goju.destroy()
  }

  "Range scan" should "return values in the range" in {
    val goju = Goju.open("test-data")
    val key1 = Utils.toBytes("range-test1")
    val value1 = Utils.toBytes("foo")
    val key2 = Utils.toBytes("range-test2")
    val value2 = Utils.toBytes("bar")
    val key3 = Utils.toBytes("range-test3")
    val value3 = Utils.toBytes("hoge")

    goju.put(key1, value1)
    goju.put(key2, value2)
    goju.put(key3, value3)
    val ret = goju.fold((k, v, acc) => {
      log.debug("Range scan, k: %s".format(k))
      val (count, list) = acc
      k match {
        case Key(k) if (k == key2 || k == key3) => (count + 1, v :: list)
        case _ => (count, list)
      }
    }, (0, List.empty[Value]))

    ret.length should be(2)
    ret.contains(value1) should be(false)
    ret.contains(value2) should be(true)
    ret.contains(value3) should be(true)
    goju.destroy()
  }

  "Add 1024 entries" should "start to beginIncrementalMerge" in {
    import org.scalatest.OptionValues._

    val goju = Goju.open("test-data")
    (1 to 1024).foreach(i => goju.put(Utils.toBytes("key" + i), Utils.toBytes("value" + i)))
    Thread.sleep(5000L)
    (1 to 1024).foreach(i => {
      log.debug("i: %d".format(i))
      goju.get(Utils.toBytes("key" + i)).value should be(Utils.toBytes("value" + i))
    })
    goju.destroy()
  }
}
