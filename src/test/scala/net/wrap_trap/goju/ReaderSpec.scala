package net.wrap_trap.goju

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import net.wrap_trap.goju.element.{Element, KeyValue}
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
    Writer.add(writer, new KeyValue(Utils.toBytes("hoge"), "hogehoge"))
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

  "lookup" should "return 'bar'" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    reader.lookup(Utils.toBytes("foo")) should be(Option("bar"))
    reader.destroy
  }

  "lookup" should "return 'hogehoge'" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    reader.lookup(Utils.toBytes("hoge")) should be(Option("hogehoge"))
    reader.destroy
  }

  "lookup" should "return None" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    reader.lookup(Utils.toBytes("fooo")) should be(None)
    reader.destroy
  }

  "rangeFold" should "return 'bar' and 'hogehoge' excluding range from and range to" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    val list = reader.rangeFold((e, acc0) => {
        e match {
          case KeyValue(_, v, _) => {v :: acc0}
        }
      }, List.empty[Element],
      KeyRange(Key(Utils.toBytes("fon")), false, Key(Utils.toBytes("hogf")), false, Integer.MAX_VALUE))
    list.size should be(2)
    list.find(p => p == "bar") should be(Some("bar"))
    list.find(p => p == "hogehoge") should be(Some("hogehoge"))
    reader.destroy
  }

  "rangeFold" should "return 'bar' and 'hogehoge' including range from and range to" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    val list = reader.rangeFold((e, acc0) => {
      e match {
        case KeyValue(_, v, _) => {v :: acc0}
      }
    }, List.empty[Element],
      KeyRange(Key(Utils.toBytes("foo")), true, Key(Utils.toBytes("hoge")), true, Integer.MAX_VALUE))
    list.size should be(2)
    list.find(p => p == "bar") should be(Some("bar"))
    list.find(p => p == "hogehoge") should be(Some("hogehoge"))
    reader.destroy
  }

  "rangeFold" should "return empty excluding range from and range to" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    val list = reader.rangeFold((e, acc0) => {
      e match {
        case KeyValue(_, v, _) => {v :: acc0}
      }
    }, List.empty[Element],
      KeyRange(Key(Utils.toBytes("foo")), false, Key(Utils.toBytes("hoge")), false, Integer.MAX_VALUE))
    list.size should be(0)
    reader.destroy
  }

  "rangeFold" should "return 'bar'" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    val list = reader.rangeFold((e, acc0) => {
      e match {
        case KeyValue(_, v, _) => {v :: acc0}
      }
    }, List.empty[Element],
      KeyRange(Key(Utils.toBytes("foo")), true, Key(Utils.toBytes("hoge")), false, Integer.MAX_VALUE))
    list.size should be(1)
    list.find(p => p == "bar") should be(Some("bar"))
    reader.destroy
  }

  "rangeFold" should "return 'hogehoge'" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    val list = reader.rangeFold((e, acc0) => {
      e match {
        case KeyValue(_, v, _) => {v :: acc0}
      }
    }, List.empty[Element],
      KeyRange(Key(Utils.toBytes("foo")), false, Key(Utils.toBytes("hoge")), true, Integer.MAX_VALUE))
    list.size should be(1)
    list.find(p => p == "hogehoge") should be(Some("hogehoge"))
    reader.destroy
  }

  "Reader.openCloseAsRandom" should "open the file for read" in writtenByRandom { fileName =>
    val reader = RandomReader.open(fileName)
    reader.name should be(fileName)
    reader.isInstanceOf[RandomReader] should be(true)
    reader.destroy
    new File(fileName).exists should be(false)
  }

  "Reader.openCloseAsSequential" should "open the file for read" in writtenBySequential { fileName =>
    val reader = SequentialReader.open(fileName)
    reader.name should be(fileName)
    reader.isInstanceOf[SequentialReader] should be(true)
    reader.destroy
    new File(fileName).exists should be(false)
  }
}
