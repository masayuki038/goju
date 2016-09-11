package net.wrap_trap.goju

import java.io.File

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.{TestKit, TestActorRef}
import net.wrap_trap.goju.element.KeyValue
import org.scalatest.{FlatSpecLike, BeforeAndAfter, Matchers, FlatSpec}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class NurserySpec extends TestKit(ActorSystem("test"))
 with FlatSpecLike with Matchers with BeforeAndAfter {

  after {
    new File("./nursery.log").delete
  }

  "newNursery" should "return new Nursery" in {
    val nursery = Nursery.newNursery(".", 1, 2)
    nursery.isInstanceOf[Nursery] should be(true)
  }

  "newNursery" should "create log file" in {
    val nursery = Nursery.newNursery(".", 1, 2)
    new File("./nursery.log").exists should be(true)
  }

  def newNursery(testCode: (Nursery, ActorRef) => Any) {
    testCode(Nursery.newNursery(".", 1, 2), TestActorRef[PlainRpcActor])
  }

  def twoInNursery(testCode: (Nursery, ActorRef) => Any) {
    val nursery = Nursery.newNursery(".", 1, 2)
    val top = TestActorRef[PlainRpcActor]
    Nursery.add(Utils.toBytes("foo"), "bar", 5, nursery, top)
    Nursery.add(Utils.toBytes("hoge"), "hogehoge", 10, nursery, top)
    testCode(nursery, top)
  }

  "Nursery.add" should "be added KeyValue" in newNursery { (nursery, top) =>
    val rawKey1 = Utils.toBytes("foo")
    Nursery.add(rawKey1, "bar", 600, nursery, top)
    val key1 = Key(rawKey1)
    nursery.tree.size should be(1)
    nursery.tree.containsKey(key1) should be(true)
    nursery.tree.get(key1).isInstanceOf[KeyValue] should be(true)
    nursery.tree.get(key1).asInstanceOf[KeyValue].value should be("bar")

    val rawKey2 = Utils.toBytes("hoge")
    Nursery.add(rawKey2, "hogehoge", 600, nursery, top)
    val key2 = Key(rawKey2)
    nursery.tree.size should be(2)
    nursery.tree.containsKey(key2) should be(true)
    nursery.tree.get(key2).isInstanceOf[KeyValue] should be(true)
    nursery.tree.get(key2).asInstanceOf[KeyValue].value should be("hogehoge")
  }

  // TODO Add Nurseary.add should KeyValue with expire time when implement Nursery.get
//  "Nursery.add" should "be added KeyValue with expire time" in newNursery { (nursery, top) =>
//    val rawKey1 = Utils.toBytes("foo")
//    Nursery.add(rawKey1, "bar", 5, nursery, top)
//    val key1 = Key(rawKey1)
//    Thread.sleep(5000L)
//    nursery.tree.size should be(1)
//    nursery.tree.containsKey(key1) should be(false)
//  }

  "Nursery.recover" should "be recoverd two elements" in twoInNursery { (nursery, top) =>
    nursery.logger.close
    val key1 = Key(Utils.toBytes("foo"))
    val key2 = Key( Utils.toBytes("hoge"))

    val newNursery = Nursery.recover(".", top, 1, 2)

    val reader = RandomReader.open(newNursery.dirPath + java.io.File.separator + Nursery.DATA_FILENAME)
    reader.lookup(Utils.toBytes("foo")) should be(Option("bar"))
    reader.lookup(Utils.toBytes("hoge")) should be(Option("hogehoge"))
    reader.destroy
  }

  "Nursery.recover" should "be recoverd two elements with expire time" in twoInNursery { (nursery, top) =>
    nursery.logger.close
    val key1 = Key(Utils.toBytes("foo"))
    val key2 = Key( Utils.toBytes("hoge"))

    val newNursery = Nursery.recover(".", top, 1, 2)
    val reader = RandomReader.open(newNursery.dirPath + java.io.File.separator + Nursery.DATA_FILENAME)

    Thread.sleep(5000L)
    reader.lookup(Utils.toBytes("foo")) should be(None)
    reader.lookup(Utils.toBytes("hoge")) should be(Option("hogehoge"))

    Thread.sleep(5000L)
    reader.lookup(Utils.toBytes("foo")) should be(None)
    reader.lookup(Utils.toBytes("hoge")) should be(None)

    reader.destroy
  }
}
