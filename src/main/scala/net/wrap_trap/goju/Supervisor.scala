package net.wrap_trap.goju

import akka.actor.{Actor, ActorRef, Props}
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
  implicit val callTimeout = Timeout(Settings.getSettings().getInt("goju.supervisor.call_timeout", 300) seconds)
  lazy val supervisor = {
    Utils.getActorSystem.actorOf(Props[Supervisor])
  }

  def createActor(props: Props, name: String): ActorRef = {
    val ret = supervisor ? (props, name)
    Await.result(ret, callTimeout.duration) match {
      case ref: ActorRef => ref
    }
  }
}

class Supervisor extends Actor {
  def receive = {
    case (props: Props, name: String) => {
      sender ! this.context.actorOf(props, name)
    }
  }
}
