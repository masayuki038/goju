package net.wrap_trap.goju

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import net.wrap_trap.goju.element.Element
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class Merge(val owner: ActorRef, val aPath: String, val bPath: String, val outPath: String, val size: Int, val isLastLevel: Boolean) extends Actor with PlainRpc {
  val log = Logger(LoggerFactory.getLogger(this.getClass))

  val aReader = SequentialReader.open(aPath)
  val bReader = SequentialReader.open(bPath)
  val out = Writer.open(outPath)
  var n = 0
  var aKVs: Option[List[Element]] = None
  var bKVs: Option[List[Element]] = None
  var fromPid: Option[(ActorRef, ActorRef)] = None

  val writerTimeout = Settings.getSettings().getInt("goju.merge.writer_timeout", 300)
  implicit val timeout = Timeout(writerTimeout seconds)

  // for scanOnly
  var cReader: Option[SequentialReader] = None
  var cKVs:Option[List[Element]] = None

  override def preStart(): Unit = {
    merge()
  }

  def merge() = {
    aKVs = aReader.firstNode match {
      case Some(members) => Option(members)
      case _ => Option(List.empty[Element])
    }
    bKVs = bReader.firstNode match {
      case Some(members) => Option(members)
      case _ => Option(List.empty[Element])
    }
    scan()
  }

  def receive = {
    case (Step, ref: ActorRef, howMany: Int) => {
      log.debug("receive Step, howMany: %d".format(howMany))
      this.n += howMany
      this.fromPid = Option((sender, ref))
      if (cReader.isEmpty) {
        scan()
      } else {
        scanOnly()
      }
    }
    case msg => throw new IllegalStateException("An unexpected message received. msg: " + msg)
    // TODO handle system messages
  }

  // Expect to call this method from "merge" only
  // Therefore, don't call back merging states to other actor
  private def scan()(implicit timeout: Timeout): Unit = {
    log.debug("scan")

    val a = aKVs.get
    val b = bKVs.get

    if(n < 1 && a.nonEmpty && b.nonEmpty) {
      log.debug("scan, n(%d) < 1 && a.nonEmpty(%d) && b.nonEmpty(%d)".format(this.n, a.size, b.size))
      fromPid.foreach { case (pid, ref) => pid ! (ref, StepDone) }
      return
    }

    if(a.isEmpty) {
      log.debug("scan, a.isEmpty")
      aReader.nextNode() match {
        case Some(a2) => {
          this.aKVs = Option(a2)
          scan()
          return
        }
        case None => {
          aReader.close()
          this.cReader = Option(this.bReader)
          this.cKVs = this.bKVs
          scanOnly()
          return
        }
      }
    }

    if(b.isEmpty) {
      log.debug("scan, b.isEmpty")
      bReader.nextNode() match {
        case Some(b2) => {
          this.bKVs = Option(b2)
          scan()
          return
        }
        case None => {
          bReader.close()
          this.cReader = Option(this.aReader)
          this.cKVs = this.aKVs
          scanOnly()
          return
        }
      }
    }

    log.debug("scan, n(%d) >= 1 and a.nonEmpty(%d) and b.nonEmpty(%d)".format(this.n, a.size, b.size))
    val aKV = a.head
    val bKV = b.head
    if(aKV.key < bKV.key) {
      log.debug("scan, aKV.key < bKV.key")
      call(out, ('add, aKV))
      this.aKVs = Option(a.tail)
      this.n -= 1
      scan()
    } else if(aKV.key > bKV.key) {
      log.debug("scan, aKV.key > bKV.key")
      call(out, ('add, bKV))
      this.bKVs = Option(b.tail)
      this.n -= 1
      scan()
    } else {
      log.debug("scan, aKV.key == bKV.key")
      call(out, ('add, bKV))
      this.aKVs = Option(a.tail)
      this.bKVs = Option(b.tail)
      this.n -= 2
      scan()
    }
  }

  // Expect to call this method from "scan" only
  // Therefore, don't call back merging states to other actor
  private def scanOnly()(implicit timeout: Timeout): Unit = {
    log.debug("scanOnly")

    val c = cKVs.get
    if(n < 1 && c.nonEmpty) {
      log.debug("scanOnly, n(%d) < 1 && c.nonEmpty(%d)".format(this.n, c.size))
      fromPid.foreach { case (pid, ref) => pid ! (ref, StepDone) }
      return
    }

    if(c.isEmpty) {
      log.debug("scanOnly, c.isEmpty")
      val reader = cReader.get
      reader.nextNode() match {
        case Some(c2) => {
          log.debug("scanOnly, c.isEmpty, reader.nextNode() is Some(c2)")
          this.cKVs = Option(c2)
          scanOnly()
          return
        }
        case None => {
          log.debug("scanOnly, c.isEmpty, reader.nextNode() is None")
          reader.close()
          val cnt = terminate()
          cast(owner, (MergeDone, cnt, outPath))
          context.stop(self)
          return
        }
      }
    }

    log.debug("scanOnly, c >= 1 and c.nonEmpty")
    val cKV = c.head
    if(!cKV.tombstoned) {
      call(out, ('add, cKV))
    }
    this.cKVs = Option(c.tail)
    this.n -= 1
    scanOnly()
  }

  private def terminate(): Int = {
    log.debug("terminate()")
    val cnt = call(out, ('count))
    call(out, 'close)
    cnt.asInstanceOf[Int]
  }
}

sealed abstract class MergeOp
case object Step extends MergeOp
case object MergeDone extends MergeOp
