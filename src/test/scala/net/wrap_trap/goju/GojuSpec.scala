package net.wrap_trap.goju

import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.TestKit
import net.wrap_trap.goju.Constants.Value
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
  val log = Logging(Utils.getActorSystem, this)

  "Open, put and get" should "return the value" in {
    import org.scalatest.OptionValues._

    TestHelper.deleteDirectory(new File("test-goju1"))
    val goju = Goju.open("test-goju1")
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

    TestHelper.deleteDirectory(new File("test-goju2"))
    val goju = Goju.open("test-goju2")
    val key = Utils.toBytes("テスト")
    val value = Utils.toBytes("太郎")
    goju.put(key, value)
    val optionValue = goju.get(key)
    optionValue should be(defined)
    optionValue.value should be(value)
    goju.destroy()
  }

  "Put one that will be expired after 2 seconds and get" should "return None" in {
    TestHelper.deleteDirectory(new File("test-goju3"))
    val goju = Goju.open("test-goju3")
    val key = Utils.toBytes("expire-test")
    val value = Utils.toBytes("hoge")
    goju.put(key, value, 2)
    Thread.sleep(3000)
    goju.get(key) should not be(defined)
    goju.destroy()
  }

  "Open, put, delete, get" should "return None" in {
    import org.scalatest.OptionValues._

    TestHelper.deleteDirectory(new File("test-goju4"))
    val goju = Goju.open("test-goju4")
    val key1 = Utils.toBytes("foo")
    val value1 = Utils.to8Bytes(77)
    goju.put(key1, value1)

    val key2 = Utils.toBytes("bar")
    val value2 = Utils.to8Bytes(89)
    goju.put(key2, value2)

    goju.delete(key1)
    val optionValue1 = goju.get(key1)
    optionValue1 shouldNot be(defined)

    val optionValue2 = goju.get(key2)
    optionValue2 should be(defined)
    optionValue2.value should be(value2)

    goju.destroy()
  }

  "Range scan" should "return values in the range" in {
    TestHelper.deleteDirectory(new File("test-goju5"))
    val goju = Goju.open("test-goju5")
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

  "To lookup 1024 entries" should "return all entries in levels" in {
    import org.scalatest.OptionValues._

    TestHelper.deleteDirectory(new File("test-goju6"))
    val goju = Goju.open("test-goju6")
    (1 to 1024).foreach(i => goju.put(Utils.toBytes("key" + i), Utils.toBytes("value" + i)))
    Thread.sleep(5000L)
    (1 to 1024).foreach(i => {
      log.debug("i: %d".format(i))
      goju.get(Utils.toBytes("key" + i)).value should be(Utils.toBytes("value" + i))
    })
    goju.destroy()
  }


}
