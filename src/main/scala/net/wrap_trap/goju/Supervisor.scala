package net.wrap_trap.goju

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
object Supervisor {
  implicit val callTimeout = Timeout(
    Settings.getSettings().getInt("goju.supervisor.call_timeout", 300) seconds)

  var maybeActorSystem: Option[ActorSystem] = None
  var maybeSupervisor: Option[ActorRef] = None

  def init(): Unit = {
    val actorSystem = ActorSystem("goju")
    maybeActorSystem = Option(actorSystem)
    maybeSupervisor = Option(actorSystem.actorOf(Props[Supervisor]))
  }

  def createActor(props: Props, name: String): ActorRef = {
    val ret = maybeSupervisor.get ? (props, name)
    Await.result(ret, callTimeout.duration) match {
      case ref: ActorRef => ref
    }
  }

  def stop(ref: ActorRef): Unit = {
    maybeActorSystem.get.stop(ref)
  }

  def stopChild(ref: ActorRef): Unit = {
    val ret = maybeSupervisor.get ? (StopChild, ref)
    Await.result(ret, callTimeout.duration) match {
      case false => throw new IllegalStateException("Failed to stopChild: %s".format(ref))
      case _ =>
    }
  }

  def terminate(): Unit = {
    val actorSystem = maybeActorSystem.get
    actorSystem.terminate
    Await.ready(actorSystem.whenTerminated, Duration(3, TimeUnit.MINUTES))
  }

  def waitForAllChildrenStopped(): Unit = {
    while (true) {
      val ret = maybeSupervisor.get ? WaitForAllChildrenStopped
      Await.result(ret, Duration.Inf) match {
        case true => return
        case _ =>
      }
      Thread.sleep(5000L)
    }
  }
}

class Supervisor extends Actor {
  val log = Logging(context.system, this)

  def receive = {
    case (props: Props, name: String) => {
      sender ! this.context.actorOf(props, name)
    }
    case (StopChild, ref: ActorRef) => {
      if (context.children.toSeq.exists(child => child == ref)) {
        context.stop(ref)
        context.sender ! true
      } else {
        sender ! false
      }
    }
    case WaitForAllChildrenStopped => {
      log.info("Supvervisor: Wait for all children stopped")
      context.children.foreach { c =>
        log.info("\tchild: %s".format(c))
      }
      sender ! (context.children.size == 0)
    }
  }
}
