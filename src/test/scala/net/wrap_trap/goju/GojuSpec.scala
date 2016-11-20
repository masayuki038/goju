package net.wrap_trap.goju

import java.io.File

import akka.actor.{Actor, Props, ActorSystem}
import akka.testkit.{TestActorRef, TestKit}
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
  with PlainRpc {

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
}
