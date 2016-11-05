package net.wrap_trap.goju

import akka.actor.{Terminated, Props, ActorRef, Actor}
import akka.util.Timeout
import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.element.KeyValue
import scala.concurrent.duration._

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class FoldRangeCoordinator(val owner: ActorRef,
                           val range: KeyRange,
                           val func: (Key, Value, (Int, List[Value])) => (Int, List[Value]),
                           var acc: (Int, List[Value]))
  extends Actor with PlainRpc {
  val callTimeout = Settings.getSettings().getInt("goju.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)
  var limit = range.limit

  def receive = {
    case (PlainRpcProtocol.call, rangeType: RangeType) => {
      val foldWorkerRef = Utils.getActorSystem.actorOf(Props(classOf[FoldWorker], self))
      context.watch(foldWorkerRef)
      call(self, (rangeType, foldWorkerRef, range))
    }
    case (PlainRpcProtocol.call, (FoldResult, _, kv: KeyValue)) => {
      val foldWorkerRef = sender
      sendReply(sender, Ok)
      this.acc = func(kv.key, kv.value, this.acc)
      this.limit -= 1
      if(this.limit <= 0) {
        context.unwatch(foldWorkerRef)
        context.stop(foldWorkerRef)
        sendReply(this.owner, this.acc)
      }
    }
    case (PlainRpcProtocol.cast, (FoldLimit, _, _)) => {
      val foldWorkerRef = sender
      context.unwatch(foldWorkerRef)
      sendReply(this.owner, this.acc)
    }
    case (PlainRpcProtocol.cast, (FoldDone, _)) => {
      val foldWorkerRef = sender
      context.unwatch(foldWorkerRef)
      sendReply(this.owner, this.acc)
    }
    case Terminated => {
      sendReply(this.owner, this.acc)
    }
  }
}
