package net.wrap_trap.goju

import java.io.File

import akka.actor.{Actor, ActorContext, ActorRef, Props}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Level extends PlainRpc {
  val log = Logger(LoggerFactory.getLogger(Level.getClass))
  val callTimeout = Settings.getSettings().getInt("goju.level.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  def open(dirPath: String, level: Int, owner: ActorRef, context: ActorContext): ActorRef = {
    // TODO This method should be moved to 'Owner'
    Utils.ensureExpiry
    context.actorOf(Props(classOf[Level], level, owner))
  }

  def level(ref: ActorRef): Any = {
    call(ref, Query)
  }

  def lookup(ref: ActorRef, key: Array[Byte]): Any = {
    call(ref, (Lookup, key))
  }

  def lookup(ref: ActorRef, key: Array[Byte], f: (Array[Byte]) => Any): Unit = {
    cast(ref, (Lookup, key, f))
  }

  def inject(ref: ActorRef, filename: String): Any = {
    call(ref, (Inject, filename))
  }

  def beginIncrementalMerge(ref: ActorRef, stepSize: Int): Any = {
    call(ref, (BeginIncrementalMerge, stepSize))
  }

  def awaitIncrementalMerge(ref: ActorRef): Any = {
    call(ref, AwaitIncrementalMerge)
  }

  def unmergedCount(ref: ActorRef): Any = {
    call(ref, UnmergedCount)
  }

  def setMaxLevel(ref: ActorRef, levelNo: Int): Unit = {
    cast(ref, (SetMaxLevel, levelNo))
  }

  def close(ref: ActorRef): Unit = {
    try {
      call(ref, Close)
    } catch {
      case ignore: Exception => {
        log.warn("Failed to close ref: " + ref, ignore)
      }
    }
  }

  def destroy(ref: ActorRef): Unit = {
    try {
      call(ref, Destroy)
    } catch {
      case ignore: Exception => {
        log.warn("Failed to close ref: " + ref, ignore)
      }
    }
  }

  def snapshotRange(ref: ActorRef, foldWorkerRef: ActorRef, keyRange: KeyRange): Unit = {
    val folders = call(ref, (InitSnapshotRangeFold, foldWorkerRef, keyRange))
    foldWorkerRef ! (Initialize, folders)
  }

  def blockingRange(ref: ActorRef, foldWorkerRef: ActorRef, keyRange: KeyRange): Unit = {
    val folders = call(ref, (InitBlockingRangeFold, foldWorkerRef, keyRange))
    foldWorkerRef ! (Initialize, folders)
  }
}

class Level(val dirPath: String, val level: Int, val owner: ActorRef) extends Actor with PlainRpc {

  var aReader: Option[RandomReader] = None
  var bReader: Option[RandomReader] = None
  var cReader: Option[RandomReader] = None
  var mergePid: Option[ActorRef] = None
  var next: Option[ActorRef] = None

  var mergeRef: Option[ActorRef] = None
  var wip: Option[Int] = None
  var workDone = 0

  override def preStart(): Unit = {
    initialize()
  }

  private def initialize(): Unit = {
    Utils.ensureExpiry
    val aFileName = filename("A")
    val bFileName = filename("B")
    val cFileName = filename("C")
    val mFileName = filename("M")

    Utils.deleteFile(filename("X"))
    Utils.deleteFile(filename("AF"))
    Utils.deleteFile(filename("BF"))
    Utils.deleteFile(filename("CF"))

    new File(mFileName).exists match {
      case true => {
        Utils.deleteFile(aFileName)
        Utils.deleteFile(bFileName)
        Utils.renameFile(mFileName, aFileName)

        this.aReader = Option(RandomReader.open(aFileName))

        new File(cFileName).exists match {
          case true => {
            Utils.renameFile(cFileName, bFileName)
            this.bReader = Option(RandomReader.open(bFileName))
            checkBeginMergeThenLoop0()
          }
          case false => {
            mainLoop()
          }
        }
      }
      case false => {
        new File(bFileName).exists match {
          case true => {
            this.aReader = Option(RandomReader.open(aFileName))
            this.bReader = Option(RandomReader.open(bFileName))
            new File(cFileName).exists match {
              case true =>
                this.cReader = Option(RandomReader.open(cFileName))
            }
          }
          case false => {
            new File(cFileName).exists match {
              case true =>
                throw new IllegalStateException("Conflict: bFileName doesn't exist but cFileName exists")
              case false => {
                new File(aFileName).exists match {
                  case true => {
                    this.aReader = Option(RandomReader.open(aFileName))
                  }
                  case false => // do nothing
                }
                mainLoop()
              }
            }
          }
        }
      }
    }
  }

  private def mainLoop(): Unit = {
    // loop
  }

  private def checkBeginMergeThenLoop0(): Unit = {
    if(aReader.isDefined && bReader.isDefined && mergePid.isEmpty) {
      val merger = beginMerge()
      val watcher = context.watch(merger)

      val progress = this.cReader match {
        case Some(r) => 2 * Utils.btreeSize(this.level)
        case _ => Utils.btreeSize(this.level)
      }
      merger ! (Step, (self, watcher), progress)

      this.mergePid = Option(merger)
      this.mergeRef = Option(watcher)
      this.wip = Option(progress)
      this.workDone = 0
      mainLoop()
    } else {
      checkBeginMergeThenLoop()
    }
  }

  private def checkBeginMergeThenLoop(): Unit = {
    if(aReader.isDefined && bReader.isDefined && mergePid.isEmpty) {
      val merger = beginMerge()
      this.mergePid = Option(merger)
      this.workDone = 0
    }
    mainLoop()
  }

  private def beginMerge(): ActorRef = {
    val aFileName = filename("A")
    val bFileName = filename("B")
    val xFileName = filename("X")

    Utils.deleteFile(xFileName)

    context.actorOf(
      Props(classOf[Merge], self, aFileName, bFileName, xFileName, Utils.btreeSize(this.level + 1), next.isEmpty)
    )
  }

  private def filename(prefix: String): String = {
    "%s%s%s-%d.data".format(dirPath, File.separator, prefix, this.level)
  }

  def receive = {
    ???
  }
}

sealed abstract class LevelOp
case object Query extends LevelOp
case object Lookup extends LevelOp
case object Inject extends LevelOp
case object BeginIncrementalMerge extends LevelOp
case object AwaitIncrementalMerge extends LevelOp
case object UnmergedCount extends LevelOp
case object SetMaxLevel extends LevelOp
case object Close extends LevelOp
case object Destroy extends LevelOp
case object InitSnapshotRangeFold extends LevelOp
case object InitBlockingRangeFold extends LevelOp

sealed abstract class FoldWorkerOp
case object Initialize extends FoldWorkerOp