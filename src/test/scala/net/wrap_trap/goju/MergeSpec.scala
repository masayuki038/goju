package net.wrap_trap.goju

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit}
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.{Element, KeyValue}
import org.hashids.Hashids
import org.scalatest._

import scala.concurrent.duration.FiniteDuration

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class MergeSpec extends TestKit(ActorSystem("test"))
  with FlatSpecLike
  with ShouldMatchers
  with StopSystemAfterAll
  with BeforeAndAfter
  with PlainRpcClient {
  implicit val hashids = Hashids.reference(this.hashCode.toString)

  val A = "a"
  val B = "b"
  val OUT = "out"

  trait Factory {
    val mergeTestActor = TestActorRef[MergeTestActor]
  }

  private def writeAndClose(path: String, kvs: List[(Key, Value)]) = {
    val fileName = new File(path).getName
    val writer = system.actorOf(Props(classOf[Writer], path, None), "writer-%s-%d".format(fileName, System.currentTimeMillis))
    for((key, value) <- kvs) {
      Writer.add(writer, new KeyValue(key, value, None))
    }
    Writer.close(writer)
    Thread.sleep(1000L)
  }

  "A1 and B1" should "merge A1 and B1" in new Factory {
    writeAndClose(A, List((Key(Utils.toBytes("foo")), "bar")))
    writeAndClose(B, List((Key(Utils.toBytes("hoge")), "hogehoge")))
    val merger = TestActorRef(Props(classOf[Merge], testActor, A, B, OUT, 0, true))
    merger ! (Step, mergeTestActor, 2)
    expectMsg(new FiniteDuration(10L, TimeUnit.SECONDS), (PlainRpcProtocol.cast, (MergeDone, 2, OUT)))
    val reader = RandomReader.open(OUT)
    reader.lookup(Utils.toBytes("foo")).get.value should be("bar")
    reader.lookup(Utils.toBytes("hoge")).get.value should be("hogehoge")
    reader.close
  }

  "A1, A2 and B1" should "merge A1, A2 and B1" in new Factory {
    writeAndClose(A, List((Key(Utils.toBytes("foo")), "bar"), (Key(Utils.toBytes("foo2")), "bar2")))
    writeAndClose(B, List((Key(Utils.toBytes("hoge")), "hogehoge")))
    val merger = TestActorRef(Props(classOf[Merge], testActor, A, B, OUT, 0, true))
    merger ! (Step, mergeTestActor, 3)
    expectMsg(new FiniteDuration(10L, TimeUnit.SECONDS), (PlainRpcProtocol.cast, (MergeDone, 3, OUT)))
    val reader = RandomReader.open(OUT)
    reader.lookup(Utils.toBytes("foo")).get.value should be("bar")
    reader.lookup(Utils.toBytes("foo2")).get.value should be("bar2")
    reader.lookup(Utils.toBytes("hoge")).get.value should be("hogehoge")
    reader.close
  }

  "A1, B1 and B2" should "merge A1, B1 and B2" in new Factory {
    writeAndClose(A, List((Key(Utils.toBytes("foo")), "bar")))
    writeAndClose(B, List((Key(Utils.toBytes("hoge")), "hogehoge"), (Key(Utils.toBytes("hoge2")), "hogehoge2")))
    val merger = TestActorRef(Props(classOf[Merge], testActor, A, B, OUT, 0, true))
    merger ! (Step, mergeTestActor, 3)
    expectMsg(new FiniteDuration(10L, TimeUnit.SECONDS), (PlainRpcProtocol.cast, (MergeDone, 3, OUT)))
    val reader = RandomReader.open(OUT)
    reader.lookup(Utils.toBytes("foo")).get.value should be("bar")
    reader.lookup(Utils.toBytes("hoge")).get.value should be("hogehoge")
    reader.lookup(Utils.toBytes("hoge2")).get.value should be("hogehoge2")
    reader.close
  }

  "A1" should "be A1" in new Factory {
    writeAndClose(A, List((Key(Utils.toBytes("foo")), "bar")))
    writeAndClose(B, List.empty[(Key, Value)])
    val merger = TestActorRef(Props(classOf[Merge], testActor, A, B, OUT, 0, true))
    merger ! (Step, mergeTestActor, 1)
    expectMsg(new FiniteDuration(10L, TimeUnit.SECONDS), (PlainRpcProtocol.cast, (MergeDone, 1, OUT)))
    val reader = RandomReader.open(OUT)
    reader.lookup(Utils.toBytes("foo")).get.value should be("bar")
    reader.lookup(Utils.toBytes("hoge")) should be(None)
    reader.close
  }

  "B1" should "be B1" in new Factory {
    writeAndClose(A, List.empty[(Key, Value)])
    writeAndClose(B, List((Key(Utils.toBytes("hoge")), "hogehoge")))
    val merger = TestActorRef(Props(classOf[Merge], testActor, A, B, OUT, 0, true))
    merger ! (Step, mergeTestActor, 1)
    expectMsg(new FiniteDuration(10L, TimeUnit.SECONDS), (PlainRpcProtocol.cast, (MergeDone, 1, OUT)))
    val reader = RandomReader.open(OUT)
    reader.lookup(Utils.toBytes("hoge")).get.value should be("hogehoge")
    reader.lookup(Utils.toBytes("foo")) should be(None)
    reader.close
  }

  after {
    new File(A).delete
    new File(B).delete
    new File(OUT).delete
  }
}

class MergeTestActor extends Actor {
  def receive = {
    case _ => {}
  }
}
