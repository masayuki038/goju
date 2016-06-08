package net.wrap_trap.goju

import akka.actor.{Actor, ActorSystem}
import akka.pattern.AskTimeoutException
import akka.testkit.{TestActorRef, TestKit}

import net.wrap_trap.goju.element.KeyValue
import org.scalatest.{FlatSpecLike, ShouldMatchers}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class WriterSpec extends TestKit(ActorSystem("test"))
  with FlatSpecLike
  with ShouldMatchers
  with StopSystemAfterAll {

  trait Factory {
    val writer = Writer.open("test")
  }

  "Writer.add" should "add a KeyValu" in new Factory {
    Writer.add(writer, new KeyValue(Utils.toBytes("foo"), "bar"))
    Thread.sleep(5000L)
    Writer.count(writer) should be(1)
  }
}
