package net.wrap_trap.goju

import java.io.File

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

  trait Factory {
    val fileName: String
    val writer = Writer.open(fileName)
    Writer.add(writer, new KeyValue(Utils.toBytes("foo"), "bar"))
    Writer.close(writer)
    Thread.sleep(1000L)
  }

  private def write(fileName: String) = {
    val writer = Writer.open(fileName)
    Writer.add(writer, new KeyValue(Utils.toBytes("foo"), "bar"))
    Writer.close(writer)
    Thread.sleep(1000L)
  }

  def writtenByRandom(testCode: (String) => Any) {
    val fileName = "random_file_test"
    write(fileName)
    testCode(fileName)
  }

  def writtenBySequential(testCode: (String) => Any) {
    val fileName = "seqential_file_test"
    write(fileName)
    testCode(fileName)
  }

  "Reader.openCloseAsRandom" should "open the file for read" in writtenByRandom { fileName =>
    val reader = Reader.open(fileName)
    reader.name should be(fileName)
    reader.isInstanceOf[RandomReadIndex] should be(true)
    Reader.destroy(reader)
    new File(fileName).exists should be(false)
  }

  "Reader.openCloseAsSequential" should "open the file for read" in writtenBySequential { fileName =>
    val reader = Reader.open(fileName, Sequential)
    reader.name should be(fileName)
    reader.isInstanceOf[SequentialReadIndex] should be(true)
    Reader.destroy(reader)
    new File(fileName).exists should be(false)
  }
}
