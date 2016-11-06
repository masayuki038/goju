package net.wrap_trap.goju

import java.io.File
import java.util.regex.Pattern

import akka.actor._
import akka.util.Timeout
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.Goju._
import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Goju extends PlainRpc {
  val callTimeout = Settings.getSettings().getInt("goju.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  def open(dirPath: String): Unit = {
    new Goju(dirPath)
  }
}

class Goju(val dirPath: String) extends Actor with PlainRpc {
  val dataFilePattern = ("^[^\\d]+-(\\d+).data$").r

  override def preStart(): Unit = {
    Utils.ensureExpiry
    val dir = new File(this.dirPath)
    val (topLevelref, nurseryRef, minLevel) = dir.isDirectory match {
      case true => {
        val (topLevel, minLevel, maxLevel) = openLevels(dir)
      }
      case false => {
        if(dir.mkdir()) {
          throw new IllegalStateException("Failed to create directory: " + dirPath)
        }
        val minLevel = Settings.getSettings().getInt("goju.level.top_level", 8)
        val topLevelRef = Level.open(this.dirPath, minLevel, None, context)
        val nurseryRef = Nursery.newNursery(this.dirPath, minLevel, minLevel)
        (topLevelRef, nurseryRef, minLevel)
      }
    }
  }

  private def openLevels(dir: File): (ActorRef, Int, Int) = {
    val topLevel0 = Settings.getSettings().getInt("goju.level.top_level", 8)
    val (minLevel, maxLevel) = dir.list.foldLeft(topLevel0, topLevel0){case ((min, max), filename: String) => {
      filename match {
        case dataFilePattern(l) => {
          val level = l.toInt
          (Math.min(min, level), Math.max(max, level))
        }
        case _ => (min, max)
      }
    }}
    val nurseryFile = new File(this.dirPath + java.io.File.pathSeparator + Nursery.DATA_FILENAME)
    if(!nurseryFile.delete) {
      throw new IllegalStateException("Failed to delete nursery file: " + nurseryFile.getAbsolutePath)
    }
    val (ref, maxMerge) =
      Range(maxLevel, minLevel).foldLeft(None: Option[ActorRef], 0){case ((nextLevel, mergeWork0), levelNo) => {
      val level = Level.open(this.dirPath, levelNo, nextLevel, context)
      (Option(level), mergeWork0 + Level.unmergedCount(level))
    }}
    val workPerIter = (maxLevel - minLevel + 1) * Utils.btreeSize(minLevel)
    val topLevelRef = ref.get
    doMerge(topLevelRef, workPerIter, maxMerge, minLevel)
    (topLevelRef, minLevel, maxLevel)
  }

  private def doMerge(topLevelRef: ActorRef, workPerIter: Int, maxMerge: Int, minLevel: Int): Unit = {
    if(maxMerge <= 0) {
      Level.awaitIncrementalMerge(topLevelRef)
    } else {
      Level.beginIncrementalMerge(topLevelRef, Utils.btreeSize(minLevel))
      doMerge(topLevelRef, workPerIter, maxMerge - workPerIter, minLevel)
    }
  }

  def close(): Unit = {
    try {
      call(self, Close)
    } catch {
      case ignore => {}
    }
  }

  def destroy(): Unit = {
    try {
      call(self, Destroy)
    } catch {
      case ignore => {}
    }
  }

  def get(key: Array[Byte]): Value = {
    call(self, (Get, key))
  }

  def lookup(key: Array[Byte]): Value = {
    get(key)
  }

  def delete(key: Array[Byte]): Unit = {
    call(self, (Delete, key))
  }

  def put(key: Array[Byte], value: Value): Unit = {
    call(self, (Put, key, value))
  }

  def transact(transactionSpecs: List[(TransactionOp, Any)]): Unit = {
    call(self, (Transact, transactionSpecs))
  }

  def fold(func: (Key, Value, (Int, List[Value])) => (Int, List[Value]), acc0: (Int, List[Value])): List[Value] = {
    foldRange(func, acc0, KeyRange(new Key(Array.empty[Byte]), true, None, true, Integer.MAX_VALUE))
  }

  def foldRange(func: (Key, Value, (Int, List[Value])) => (Int, List[Value]),
                acc0: (Int, List[Value]),
                range: KeyRange): List[Value] = {
    val rangeType = range.limit < 10 match {
      case true => BlockingRange
      case false => SnapshotRange
    }
    val coordinatorRef = Utils.getActorSystem.actorOf(Props(classOf[FoldRangeCoordinator], self, range, func, acc0))
    call(self, rangeType) match {
      case results: List[Value] => {
        context.stop(coordinatorRef)
        results
      }
    }
  }

  def receive = {
    case _ =>
  }
}

sealed abstract class GojuOp
case object Get extends GojuOp
case object Transact extends GojuOp

sealed abstract class RangeType
case object BlockingRange extends RangeType
case object SnapshotRange extends RangeType