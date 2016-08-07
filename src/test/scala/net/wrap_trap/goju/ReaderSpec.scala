package net.wrap_trap.goju

import akka.actor.ActorSystem
import akka.testkit.TestKit
import net.wrap_trap.goju.element.KeyValue
import org.scalatest._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class ReaderSpec extends TestKit(ActorSystem("test"))
  with FlatSpecLike
  with ShouldMatchers
  with StopSystemAfterAll {

  "Reader.openAsRandom" should "open the file for read" in {
    val writer = Writer.open("random_file_test")
    Writer.add(writer, new KeyValue(Utils.toBytes("foo"), "bar"))
    Writer.close(writer)
    Thread.sleep(1000L)
    val reader = Reader.open("test")
    reader.name should be("test")
    reader.isInstanceOf[RandomReadIndex] should be(true)
  }

  "Reader.openAsSequential" should "open the file for read" in {
    val writer = Writer.open("seqential_file_test")
    Writer.add(writer, new KeyValue(Utils.toBytes("foo"), "bar"))
    Writer.close(writer)
    Thread.sleep(1000L)
    val reader = Reader.open("test", Sequential)
    reader.name should be("test")
    reader.isInstanceOf[SequentialReadIndex] should be(true)
  }

}
