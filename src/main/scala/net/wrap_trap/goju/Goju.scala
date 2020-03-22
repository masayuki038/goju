package net.wrap_trap.goju

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.util.Timeout
import akka.event.{LogSource, Logging}
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.Goju._
import net.wrap_trap.goju.element.KeyValue

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */

class GojuLogSource extends LogSource[AnyRef] {
  def genString(o: AnyRef): String = o.getClass.getName
  override def getClazz(o: AnyRef): Class[_] = o.getClass
}

object Goju extends PlainRpcClient {
  val log = Logging(Utils.getActorSystem, this)

  val callTimeout = Settings.getSettings().getInt("goju.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  def open(dirPath: String): Goju = {
    val goju = new Goju(dirPath)
    goju.init()
    goju
  }
}

class Goju(val dirPath: String) extends PlainRpcClient {
  val log = Logging(Utils.getActorSystem, this)

  val dataFilePattern = ("^[^\\d]+-(\\d+).data$").r
  var nursery: Option[Nursery] = None
  var topLevelRef: Option[ActorRef] = None
  var maxLevel: Option[Int] = None

  def init(): Unit = {
    log.debug("init")
    Utils.ensureExpiry
    val dir = new File(this.dirPath)
    val (topRef, newNursery, maxLevel) = dir.isDirectory match {
      case true => {
        log.debug("init: data directory already exists")
        val (topRef, minLevel, maxLevel) = openLevels(dir)
        val newNursery = Nursery.recover(this.dirPath, topRef, minLevel, maxLevel)
        (topRef, newNursery, maxLevel)
      }
      case false => {
        log.debug("init: data directory does not exist")
        if(!dir.mkdir()) {
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
    log.debug("init: this.topLevelRef.get: %s".format(this.topLevelRef.get))
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
    log.info("minLevel: %d, maxLevel: %d".format(minLevel, maxLevel))
    val nurseryFile = new File(this.dirPath + java.io.File.separator + Nursery.DATA_FILENAME)
    if(nurseryFile.exists && !nurseryFile.delete()) {
      throw new IllegalStateException("Failed to delete nursery file: " + nurseryFile.getAbsolutePath)
    }
    val (ref, maxMerge) =
      Range(maxLevel, minLevel).foldLeft(None: Option[ActorRef], 0){case ((nextLevel, mergeWork0), levelNo) => {
      val level = Level.open(this.dirPath, levelNo, nextLevel)
      (Option(level), mergeWork0 + Level.unmergedCount(level))
    }}
    val workPerIter = (maxLevel - minLevel + 1) * Utils.btreeSize(minLevel)
    val topLevelRef = ref match {
      case Some(r) => r
      case _ => {
        log.error("Failed to get topLevelRef. data files are: ")
        dir.listFiles.foreach(f => log.error(f.getAbsolutePath))
        throw new IllegalStateException("Failed to get topLevelRef")
      }
    }
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
    log.debug("close")
    log.debug("this.topLevelRef.get: %s".format(this.topLevelRef.get))
    try {
      Nursery.finish(this.nursery.get, this.topLevelRef.get)
      Level.close(this.topLevelRef.get)
    } catch {
      case ignore: Exception => {
        log.error(ignore, "Failed to Goju#close")
      }
    }
  }

  def stopLevel(): Unit = {
    log.debug("stopLevel")
    Supervisor.stopChild((this.topLevelRef.get))
  }

  def terminate(): Unit = {
    val system = Utils.getActorSystem
    system.terminate
    Await.ready(system.whenTerminated, Duration(3, TimeUnit.MINUTES))
  }

  def destroy(): Unit = {
    try {
      val topLevelNumber = Level.level(topLevelRef.get)
      this.nursery.get.destroy()
      Level.destroy(this.topLevelRef.get)
      this.maxLevel = Option(topLevelNumber)
    } catch {
      case ignore: Exception => {
        log.warning("Failed to Goju#destroy", ignore)
      }
    }
  }

  def get(key: Array[Byte]): Option[Value] = {
    log.debug("get key: %s".format(key))
    this.nursery.get.lookup(key) match {
      case Some(e) if e.tombstoned || e.expired => None
      case Some(e) => Option(e.value)
      case None => {
        Level.lookup(this.topLevelRef.get, key) match {
          case Some(kv: KeyValue) => {
            Option(kv.value)
          }
          case None => None
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
    this.nursery = Option(Nursery.add(key, value, this.nursery.get, this.topLevelRef.get))
  }

  def put(key: Array[Byte], value: Value, keyExpireSecs: Int): Unit = {
    this.nursery = Option(Nursery.add(key, value, keyExpireSecs, this.nursery.get, this.topLevelRef.get))
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
      Props(classOf[FoldRangeCoordinator], this.topLevelRef.get, this.nursery.get, range, func, acc0),
      "foldRangeCoordinator-" + System.currentTimeMillis)
    call(coordinatorRef, Start) match {
      case (count, results: List[Value]) => {
        log.debug("foldRange, replied %d values".format(count))
        system.stop(coordinatorRef)
        results
      }
    }
  }
}