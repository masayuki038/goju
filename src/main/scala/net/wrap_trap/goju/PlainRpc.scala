package net.wrap_trap.goju

import akka.actor.{Actor, ActorContext, ActorRef}
import net.wrap_trap.goju.PlainRpcProtocol._

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
trait PlainRpc extends Actor with PlainRpcClient {
  def sendCall(pid: ActorRef, context: ActorContext, msg: Any): ActorRef = {
    plainRpcLog.debug("sendCall, pid: %s, context: %s, msg: %s".format(pid, context, msg))
    val monitor = context.watch(pid)
    pid ! CALL(msg)
    monitor
  }

  def sendReply(source: ActorRef, reply: Any) = {
    plainRpcLog.debug("sendReply, source: %s, reply: %s".format(source, reply))
    source ! REPLY(reply)
  }
}
