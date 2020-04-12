package net.wrap_trap.goju

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.AskTimeoutException
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import java.io.File

import net.wrap_trap.goju.element.KeyValue
import org.scalatest.FlatSpecLike
import org.scalatest.ShouldMatchers

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
class WriterSpec
    extends TestKit(ActorSystem("test")) with FlatSpecLike with ShouldMatchers
    with StopSystemAfterAll {

  trait Factory {
    private val fileName = new File("test-data/test").getName

    val writer: ActorRef = system.actorOf(
      Props(classOf[Writer], fileName, None),
      "writer-%s-%d".format(fileName, System.currentTimeMillis))
  }

  "Writer.add" should "add a KeyValue" in new Factory {
    Writer.add(writer, new KeyValue(Utils.toBytes("foo"), "bar"))
    Thread.sleep(5000L)
    Writer.count(writer) should be(1)
  }

  "Writer.close" should "archive nodes and bloom" in new Factory {
    Writer.add(writer, new KeyValue(Utils.toBytes("foo"), "bar"))
    Writer.close(writer)
    Thread.sleep(5000L)
  }
}
