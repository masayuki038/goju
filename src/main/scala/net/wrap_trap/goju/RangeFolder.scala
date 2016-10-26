package net.wrap_trap.goju

import akka.actor.{ActorRef, Actor}
import akka.util.Timeout
import net.wrap_trap.goju.Constants.FOLD_CHUNK_SIZE
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.KeyValue

import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class RangeFolder(fileName: String, workerPid: ActorRef, owner: ActorRef, range: KeyRange) extends Actor with PlainRpc {
  val callTimeout = Settings.getSettings().getInt("goju.level.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  override def preStart() = {
    val reader = RandomReader.open(fileName)
    doRangeFold2(reader, workerPid, self, range)
    reader.close()

    owner ! (RangeFoldDone, self, fileName)
  }

  private def doRangeFold2(reader: RandomReader, workderPid: ActorRef, selfOrRef: ActorRef, range: KeyRange): Unit = {
    reader.rangeFold((keyValue, acc0) => {
        (keyValue, acc0) match {
          case (e: KeyValue, (0, acc)) => {
            send(workerPid, selfOrRef, e.value :: acc)
            (FOLD_CHUNK_SIZE, List.empty[Value])
          }
          case (e: KeyValue, (f, acc)) => {
            ((f - 1), e.value :: acc)
          }
        }
      },
      (FOLD_CHUNK_SIZE - 1, List.empty[Value]),
      range)
  }

  def send(workerPid: ActorRef, selfOrRef: ActorRef, reverseValues: List[Value]): Unit = {
    call(workerPid, (LevelResults, selfOrRef, reverseValues.reverse))
  }

  def receive = {
  // do nothing
    case _ => {}
  }
}
