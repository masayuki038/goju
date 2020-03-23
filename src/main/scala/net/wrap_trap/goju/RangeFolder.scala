package net.wrap_trap.goju

import akka.actor.{ActorRef, Actor}
import akka.event.Logging
import akka.util.Timeout
import net.wrap_trap.goju.Constants.FOLD_CHUNK_SIZE
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.{Element, KeyValue}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class RangeFolder(filePath: String, workerPid: ActorRef, owner: ActorRef, range: KeyRange) extends PlainRpc {
  val callTimeout = Settings.getSettings().getInt("goju.level.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  override def preStart() = {
    val reader = RandomReader.open(filePath)
    doRangeFold2(reader, workerPid, self, range)
    reader.close()

    owner ! (RangeFoldDone, self, filePath)
    context.stop(self)
  }

  private def doRangeFold2(reader: RandomReader, workderPid: ActorRef, selfOrRef: ActorRef, range: KeyRange): Unit = {
    log.debug("doRangeFold2, reader: %s, workerPid: %s, selfOrRef: %s, range: %s".format(reader, workerPid, selfOrRef, range))
    val (_, valueList) = reader.rangeFold((keyValue, acc0) => {
        (keyValue, acc0) match {
          case (e: KeyValue, (0, acc)) => {
            log.debug("doRangeFold2, f: 0")
            send(workerPid, selfOrRef, e :: acc)
            (FOLD_CHUNK_SIZE, List.empty[Element])
          }
          case (e: KeyValue, (f, acc)) => {
            log.debug("doRangeFold2, f: %d".format(f))
            ((f - 1), e :: acc)
          }
          case unexpected => throw new IllegalStateException("Unexpected value: %s".format(unexpected))
        }
      },
      (FOLD_CHUNK_SIZE - 1, List.empty[Element]),
      range)
    send(workerPid, selfOrRef, valueList)
    workerPid ! (LevelDone, selfOrRef)
  }

  def send(workerPid: ActorRef, selfOrRef: ActorRef, reverseKvs: List[Element]): Unit = {
    log.debug("send, workerPid: %s, selfOrRef: %s, reverseKvs.size: %d".format(workerPid, selfOrRef, reverseKvs.size))
    call(workerPid, LevelResults(selfOrRef, reverseKvs.reverse))
  }

  def receive = {
  // do nothing
    case _ => {}
  }
}
