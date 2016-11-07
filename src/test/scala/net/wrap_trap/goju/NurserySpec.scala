package net.wrap_trap.goju

import java.io.File

import akka.actor.{Actor, ActorSystem, ActorRef}
import akka.testkit.{TestKit, TestActorRef}
import com.typesafe.scalalogging.Logger
import net.wrap_trap.goju.element.KeyValue
import org.scalatest.{FlatSpecLike, BeforeAndAfter, Matchers}
import org.slf4j.LoggerFactory

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class NurserySpec extends TestKit(ActorSystem("test"))
 with FlatSpecLike with Matchers with BeforeAndAfter {
  val log = Logger(LoggerFactory.getLogger(Nursery.getClass))

  after {
    val deleted = new File("./" +  Nursery.DATA_FILENAME).delete
    log.info("Nursery's data has deleted: " + deleted)
  }

  "newNursery" should "return new Nursery" in {
    val nursery = Nursery.newNursery(".", 1, 2)
    nursery.isInstanceOf[Nursery] should be(true)
    nursery.destroy()
  }

  "newNursery" should "create log file" in {
    val nursery = Nursery.newNursery(".", 1, 2)
    new File("./nursery.log").exists should be(true)
    nursery.destroy()
  }

  def newNursery(testCode: (Nursery, ActorRef) => Any) {
    testCode(Nursery.newNursery(".", 1, 2), TestActorRef[LevelSutbForRecover])
  }

  def twoInNursery(testCode: (Nursery, ActorRef) => Any) {
    val nursery = Nursery.newNursery(".", 1, 2)
    val top = TestActorRef[LevelSutbForRecover]
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
    nursery.destroy()
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
    nursery.logger.close()
    val newNursery = Nursery.recover(".", top, 1, 2)

    val reader = RandomReader.open(newNursery.dirPath + java.io.File.separator + Nursery.DATA_FILENAME)
    reader.lookup(Utils.toBytes("foo")) should be(Option("bar"))
    reader.lookup(Utils.toBytes("hoge")) should be(Option("hogehoge"))
    reader.destroy()
    newNursery.destroy()
  }

  "Nursery.recover" should "be recoverd two elements with expire time" in twoInNursery { (nursery, top) =>
    nursery.logger.close()
    val newNursery = Nursery.recover(".", top, 1, 2)
    val reader = RandomReader.open(newNursery.dirPath + java.io.File.separator + Nursery.DATA_FILENAME)

    Thread.sleep(5000L)
    reader.lookup(Utils.toBytes("foo")) should be(None)
    reader.lookup(Utils.toBytes("hoge")) should be(Option("hogehoge"))

    Thread.sleep(5000L)
    reader.lookup(Utils.toBytes("foo")) should be(None)
    reader.lookup(Utils.toBytes("hoge")) should be(None)

    reader.destroy()
    newNursery.destroy()
  }
}

class LevelSutbForRecover extends Actor with PlainRpc {

  def receive = {
    case (PlainRpcProtocol.call, (Inject, fileName: String)) => {
      sendReply(sender(), Ok)
    }
    case (PlainRpcProtocol.call, (BeginIncrementalMerge, stepSize: Int)) => {
      sendReply(sender(), true)
    }
  }
}
