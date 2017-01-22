package net.wrap_trap.goju

import akka.actor.{Terminated, Props, ActorRef, Actor}
import akka.util.Timeout
import akka.event.Logging
import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.element.KeyValue
import scala.concurrent.duration._

import org.hashids.Hashids
import org.hashids.syntax._

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class FoldRangeCoordinator(val topLevelRef: ActorRef,
                           val nursery: Nursery,
                           val range: KeyRange,
                           val func: (Key, Value, (Int, List[Value])) => (Int, List[Value]),
                           var acc: (Int, List[Value]))
  extends Actor with PlainRpc {
  val log = Logging(context.system, this)
  implicit val hashids = Hashids.reference(this.hashCode.toString)

  val callTimeout = Settings.getSettings().getInt("goju.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)
  var limit = range.limit
  var owner: Option[ActorRef] = None

  def receive = {
    case (PlainRpcProtocol.call, Start) => {
      log.debug("receive Start")
      this.owner = Option(sender)
      val foldWorkerRef = Utils.getActorSystem.actorOf(
        Props(classOf[FoldWorker], self),
        "foldWorker-" + System.currentTimeMillis)
      context.watch(foldWorkerRef)

      foldWorkerRef ! Prefix(List(self))

      range.limit < 10 match {
        case true => {
          // BlockingRange
          Level.blockingRange(this.topLevelRef, foldWorkerRef, range)
          this.nursery.doLevelFold(foldWorkerRef, self, range)
        }
        case false => {
          // SnapshotRange
          Level.snapshotRange(this.topLevelRef, foldWorkerRef, range)
          this.nursery.doLevelFold(foldWorkerRef, self, range)
        }
      }
    }
    case (PlainRpcProtocol.call, (FoldResult, _, kv: KeyValue)) => {
      log.debug("receive FoldResult, kv: %s".format(kv))
      val foldWorkerRef = sender
      sendReply(sender, Ok)
      this.acc = func(kv.key, kv.value, this.acc)
      this.limit -= 1
      if(this.limit <= 0) {
        context.unwatch(foldWorkerRef)
        context.stop(foldWorkerRef)
        sendReply(this.owner.get, this.acc)
      }
    }
    case (PlainRpcProtocol.cast, (FoldLimit, _, _)) => {
      log.debug("receive FoldLimit")
      val foldWorkerRef = sender
      context.unwatch(foldWorkerRef)
      sendReply(this.owner.get, this.acc)
    }
    case (PlainRpcProtocol.cast, (FoldDone, _)) => {
      log.debug("receive FoldDone")
      val foldWorkerRef = sender
      context.unwatch(foldWorkerRef)
      sendReply(this.owner.get, this.acc)
    }
    case Terminated => {
      log.debug("receive Terminated")
      sendReply(this.owner.get, this.acc)
    }
  }
}
