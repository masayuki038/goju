package net.wrap_trap.goju

import java.io.File

import akka.actor._
import akka.util.Timeout
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.Goju._
import net.wrap_trap.goju.element.KeyValue
import org.joda.time.DateTime

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
    new Goju(dirPath).init()
  }
}

class Goju(val dirPath: String) extends PlainRpc {
  val dataFilePattern = ("^[^\\d]+-(\\d+).data$").r
  var nursery: Option[Nursery] = None
  var topLevelRef: Option[ActorRef] = None
  var maxLevel: Option[Int] = None

  def init(): Unit = {
    Utils.ensureExpiry
    val dir = new File(this.dirPath)
    val (topRef, newNursery, maxLevel) = dir.isDirectory match {
      case true => {
        val (topRef, minLevel, maxLevel) = openLevels(dir)
        val newNursery = Nursery.recover(this.dirPath, topRef, minLevel, maxLevel)
        (topRef, newNursery, maxLevel)
      }
      case false => {
        if(dir.mkdir()) {
          throw new IllegalStateException("Failed to create directory: " + dirPath)
        }
        val minLevel = Settings.getSettings().getInt("goju.level.top_level", 8)
        val topLevelRef = Level.open(this.dirPath, minLevel, None)
        val newNursery = Nursery.newNursery(this.dirPath, minLevel, minLevel)
        (topLevelRef, newNursery, minLevel)
      }
    }
    this.nursery = Option(newNursery)
    this.topLevelRef = Option(topRef)
    this.maxLevel = Option(maxLevel)
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
      val level = Level.open(this.dirPath, levelNo, nextLevel)
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
      Nursery.finish(this.nursery.get, this.topLevelRef.get)
      val min = Level.level(this.topLevelRef.get)
      this.nursery = Option(Nursery.newNursery(this.dirPath, min, this.maxLevel.get))
      Level.close(this.topLevelRef.get)
    } catch {
      case ignore => {}
    }
  }

  def destroy(): Unit = {
    try {
      val topLevelNumber = Level.level(topLevelRef.get)
      this.nursery.get.destroy()
      Level.destroy(this.topLevelRef.get)
      this.maxLevel = Option(topLevelNumber)
    } catch {
      case ignore => {}
    }
  }

  def get(key: Array[Byte]): Option[Value] = {
    this.nursery.get.lookup(key) match {
      case Some(e) => Option(e.value)
      case None => {
        Level.lookup(this.topLevelRef.get, key) match {
          case kv: KeyValue => {
            Option(kv.value)
          }
          case _ => None
        }
      }
    }
  }

  def lookup(key: Array[Byte]): Option[Value] = {
    get(key)
  }

  def delete(key: Array[Byte]): Unit = {
    put(key, Constants.TOMBSTONE)
  }

  def put(key: Array[Byte], value: Value): Unit = {
    Nursery.add(key, value, this.nursery.get, this.topLevelRef.get)
  }

  def put(key: Array[Byte], value: Value, keyExpireSecs: Int): Unit = {
    Nursery.add(key, value, keyExpireSecs, this.nursery.get, this.topLevelRef.get)
  }

  def transact(transactionSpecs: List[(TransactionOp, Any)]): Unit = {
    this.nursery.get.transact(transactionSpecs, this.topLevelRef.get)
  }

  def fold(func: (Key, Value, (Int, List[Value])) => (Int, List[Value]), acc0: (Int, List[Value])): List[Value] = {
    foldRange(func, acc0, KeyRange(new Key(Array.empty[Byte]), true, None, true, Integer.MAX_VALUE))
  }

  def foldRange(func: (Key, Value, (Int, List[Value])) => (Int, List[Value]),
                acc0: (Int, List[Value]),
                range: KeyRange): List[Value] = {
    val system = Utils.getActorSystem
    val coordinatorRef = system.actorOf(
      Props(classOf[FoldRangeCoordinator], this.topLevelRef.get, this.nursery.get, range, func, acc0))
    call(coordinatorRef, Start) match {
      case results: List[Value] => {
        system.stop(coordinatorRef)
        results
      }
    }
  }
}

sealed abstract class GojuOp
case object Get extends GojuOp
case object Transact extends GojuOp

sealed abstract class RangeOp
case object Start extends RangeOp

sealed abstract class RangeType
case object BlockingRange extends RangeType
case object SnapshotRange extends RangeType