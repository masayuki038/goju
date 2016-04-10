package net.wrap_trap.goju

import akka.actor.ActorSystem
import akka.pattern.AskTimeoutException
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import org.scalatest.{FlatSpecLike, ShouldMatchers}

import scala.concurrent.duration._

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class PlainRpcSpec extends TestKit(ActorSystem("test"))
  with FlatSpecLike
  with ShouldMatchers
  with StopSystemAfterAll {

  trait Factory {
    val actor1 = TestActorRef[PlainRpcActor]
    val actor2 = TestActorRef[PlainRpcActor]
  }

  "PlainRpc.sendCall" should "include 'CALL" in new Factory {
    actor1.underlyingActor.sendCall(actor2, "foo")
    actor2.underlyingActor.lastType should equal('CALL)
    actor2.underlyingActor.lastMessage should be("foo")
  }

  "PlainRpc.cast" should "include 'CAST" in new Factory {
    actor1.underlyingActor.cast(actor2, "bar")
    actor2.underlyingActor.lastType should equal('CAST)
    actor2.underlyingActor.lastMessage should be("bar")
  }

  "PlainRpc.call" should "include 'CALL" in new Factory {
    implicit val timeout = Timeout(5 seconds)
    val ret = actor1.underlyingActor.call(actor2, "bar")
    ret should be("PlainRpcActor#reply")
  }

  "PlainRpc.call" should "throw AskTimeoutException when actor2 is dead" in new Factory {
    actor2.stop()
    implicit val timeout = Timeout(5 seconds)
    intercept[AskTimeoutException] {
      val ret = actor1.underlyingActor.call(actor2, "bar")
    }
  }
}

class PlainRpcActor extends PlainRpc {

  var messageType: Symbol = _
  var message: String = _

  def receive = {
    case (PlainRpcProtocol.cast, msg: String) => {
      println("receive: cast: " + msg)
      messageType = PlainRpcProtocol.cast
      message = msg
    }
    case (PlainRpcProtocol.call, msg: String) => {
      println("receive: call: " + msg)
      messageType = PlainRpcProtocol.call
      message = msg
      sendReply("PlainRpcActor#reply")
    }
  }

  def lastType = messageType

  def lastMessage = message
}
