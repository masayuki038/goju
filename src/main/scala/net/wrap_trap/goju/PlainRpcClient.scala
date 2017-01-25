package net.wrap_trap.goju

import akka.actor.{ActorContext, ActorRef}
import akka.event.{LogSource, Logging}
import akka.pattern.ask
import akka.util.Timeout
import net.wrap_trap.goju.PlainRpcProtocol._

import scala.concurrent.Await

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
trait PlainRpcClient {
  implicit val logSource: LogSource[AnyRef] = new GojuLogSource()
  val plainRpcLog = Logging(Utils.getActorSystem, this)

  def cast(pid: ActorRef, msg: Any) = {
    pid ! CAST(msg)
  }

  def call(pid: ActorRef, request: Any)(implicit timeout: Timeout): Any = {
    val ret = pid ? CALL(request)
    Await.result(ret, timeout.duration) match {
      case (PlainRpcProtocol.reply, reply: Any) => reply
      case x: Any => throw new IllegalStateException("Unexpected message: " + x)
    }
  }
}
