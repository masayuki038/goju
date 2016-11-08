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
    val ret = new File("test-data").delete()
    println("deleted: " + ret)
  }

  "Open, put and get" should "return a value" in {
    val goju = Goju.open("test-data")
    val key = Utils.toBytes("data")
    val value = Utils.to8Bytes(77)
    goju.put(key, value)
    val ret = goju.get(key)
    ret shouldBe Some(value)
  }
}
