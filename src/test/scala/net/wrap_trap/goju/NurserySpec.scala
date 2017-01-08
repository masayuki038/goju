package net.wrap_trap.goju

import java.io.File

import akka.actor.{Actor, ActorSystem, ActorRef}
import akka.event.{LogSource, Logging}
import akka.testkit.{TestKit, TestActorRef}
import net.wrap_trap.goju.element.KeyValue
import org.scalatest.{FlatSpecLike, BeforeAndAfter, Matchers}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class NurserySpec extends TestKit(ActorSystem("test"))
 with FlatSpecLike with Matchers with BeforeAndAfter {
  implicit val logSource: LogSource[AnyRef] = new GojuLogSource()
  val log = Logging(Utils.getActorSystem, this)

  "newNursery" should "return new Nursery" in {
    TestHelper.remakeDir(new File("test-nursery1"))
    val nursery = Nursery.newNursery("test-nursery1", 8, 8)
    nursery.isInstanceOf[Nursery] should be(true)
    nursery.destroy()
  }

  "newNursery" should "create log file" in {
    TestHelper.remakeDir(new File("test-nursery2"))
    val nursery = Nursery.newNursery("test-nursery2", 8, 8)
    new File("./test-nursery2/nursery.log").exists should be(true)
    nursery.destroy()
  }

  def newNursery(dir: String, testCode: (Nursery, ActorRef) => Any) {
    TestHelper.remakeDir(new File(dir))
    testCode(Nursery.newNursery(dir, 8, 8), TestActorRef[LevelSutbForRecover])
  }

  def twoInNursery(dir: String, testCode: (Nursery, ActorRef) => Any) {
    TestHelper.remakeDir(new File(dir))
    val nursery = Nursery.newNursery(".", 8, 8)
    val top = TestActorRef[LevelSutbForRecover]
    Nursery.add(Utils.toBytes("foo"), "bar", 5, nursery, top)
    Nursery.add(Utils.toBytes("hoge"), "hogehoge", 10, nursery, top)
    testCode(nursery, top)
  }

  "Nursery.add" should "be added KeyValue" in newNursery("test-nursery3", { (nursery, top) =>
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
  })

  "Nursery.recover" should "be recoverd two elements" in twoInNursery("test-nursery4", { (nursery, top) =>
    nursery.logger.close()
    val newNursery = Nursery.recover(".", top, 1, 2)

    val reader = RandomReader.open(newNursery.dirPath + java.io.File.separator + Nursery.DATA_FILENAME)
    val kv1 = reader.lookup(Utils.toBytes("foo"))
    kv1 should be(defined)
    kv1.get.value should be("bar")
    val kv2 =  reader.lookup(Utils.toBytes("hoge"))
    kv2 should be(defined)
    kv2.get.value should be("hogehoge")
    reader.destroy()
    newNursery.destroy()
  })

  "Nursery.recover" should "be recoverd two elements with expire time" in twoInNursery("test-nursery5", { (nursery, top) =>
    nursery.logger.close()
    val newNursery = Nursery.recover(".", top, 1, 2)
    val reader = RandomReader.open(newNursery.dirPath + java.io.File.separator + Nursery.DATA_FILENAME)

    Thread.sleep(5000L)
    reader.lookup(Utils.toBytes("foo")) should be(None)
    val ret1 = reader.lookup(Utils.toBytes("hoge"))
    ret1 should be(defined)
    ret1.get.value should be("hogehoge")

    Thread.sleep(5000L)
    reader.lookup(Utils.toBytes("foo")) should be(None)
    reader.lookup(Utils.toBytes("hoge")) should be(None)

    reader.destroy()
    newNursery.destroy()
  })
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
