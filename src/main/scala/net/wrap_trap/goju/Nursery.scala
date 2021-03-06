package net.wrap_trap.goju

import akka.actor.ActorRef
import org.slf4j.LoggerFactory

import collection.JavaConversions._
import java.io.File
import java.io.FileOutputStream
import java.util
import java.util.TreeMap

import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.Element
import net.wrap_trap.goju.element.KeyValue

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
object Nursery {
  private val log = LoggerFactory.getLogger(this.getClass)

  val LOG_FILENAME = "nursery.log"
  val DATA_FILENAME = "nursery.data"

  def newNursery(dirPath: String, minLevel: Int, maxLevel: Int): Nursery = {
    log.debug("newNursery")
    Utils.ensureExpiry()
    new Nursery(dirPath, minLevel, maxLevel)
  }

  def flush(nursery: Nursery, top: ActorRef): Nursery = {
    log.debug("flush")
    finish(nursery, top)
    val logFile = new File(nursery.dirPath + java.io.File.separator + Nursery.LOG_FILENAME)
    if (logFile.exists) {
      throw new IllegalStateException("Failed to delete log file")
    }
    newNursery(nursery.dirPath, nursery.minLevel, nursery.maxLevel)
  }

  def add(key: Array[Byte], value: Value, nursery: Nursery, top: ActorRef): Nursery = {
    add(key, value, 0, nursery, top)
  }

  def add(
      key: Array[Byte],
      value: Value,
      keyExpireSecs: Int,
      nursery: Nursery,
      top: ActorRef): Nursery = {
    if (nursery.doAdd(key, value, keyExpireSecs, top)) {
      nursery
    } else {
      flush(nursery, top)
    }
  }

  def recover(dirPath: String, topLevel: ActorRef, minLevel: Int, maxLevel: Int): Nursery = {
    if (minLevel > maxLevel) {
      throw new IllegalArgumentException(s"""$minLevel(minLevel) > $maxLevel(maxLevel)""")
    }
    Utils.ensureExpiry()

    val logFile = new File(dirPath + java.io.File.separator + LOG_FILENAME)
    if (logFile.exists()) {
      doRecover(logFile, topLevel, minLevel, maxLevel)
    }
    new Nursery(dirPath, minLevel, maxLevel)
  }

  def doRecover(logFile: File, topLevel: ActorRef, minLevel: Int, maxLevel: Int): Unit = {
    log.debug("doRecover: minLevel: %d, maxLevel: %d".format(minLevel, maxLevel))
    val nursery = readNurseryFromLog(logFile, minLevel, maxLevel)
    finish(nursery, topLevel)
    if (logFile.exists) {
      throw new IllegalStateException("Failed to delete log file in recover")
    }
  }

  def ensureSpace(nursery: Nursery, neededRooms: Int, top: ActorRef): Nursery = {
    log.debug("ensureSpace: neededRooms: %d".format(neededRooms))
    if (nursery.hasRoom(neededRooms)) {
      nursery
    } else {
      flush(nursery, top)
    }
  }

  def finish(nursery: Nursery, topLevel: ActorRef): Unit = {
    log.debug("finish: nursery: %s, topLevel: %s".format(nursery, topLevel))
    Utils.ensureExpiry()

    if (nursery.tree.size > 0) {
      val btreeFileName = nursery.dirPath + java.io.File.separator + DATA_FILENAME
      val writer = Writer.open(btreeFileName)
      try {
        nursery.tree.foreach {
          case (_, e) =>
            Writer.add(writer, e)
        }
      } finally {
        Writer.close(writer)
      }
      Level.inject(topLevel, btreeFileName)
      if (nursery.mergeDone < Utils.btreeSize(nursery.minLevel)) {
        Level.beginIncrementalMerge(topLevel, Utils.btreeSize(nursery.minLevel) - nursery.mergeDone)
      }
    }
    nursery.destroy()
  }

  private def readNurseryFromLog(logFile: File, minLevel: Int, maxLevel: Int): Nursery = {
    val logBinary = java.nio.file.Files.readAllBytes(logFile.toPath)
    val recovered = Utils.decodeCRCData(logBinary, List.empty[Array[Byte]], List.empty[Element])
    val tree = new util.TreeMap[Key, Element]
    for (e <- recovered) { if (!e.tombstoned) { tree.put(e.key(), e) } }
    new Nursery(logFile.getParent, minLevel, maxLevel, tree)
  }
}

class Nursery(
    val dirPath: String,
    val minLevel: Int,
    val maxLevel: Int,
    val tree: util.TreeMap[Key, Element]) {
  private val log = LoggerFactory.getLogger(this.getClass)

  val logger = new FileOutputStream(dirPath + java.io.File.separator + Nursery.LOG_FILENAME, true)
  log.debug("%s created.".format(dirPath + java.io.File.separator + Nursery.LOG_FILENAME))
  private var lastSync = System.currentTimeMillis
  var step = 0
  var mergeDone = 0
  var totalSize = 0

  def this(dirPath: String, minLevel: Int, maxLevel: Int) = {
    this(dirPath, minLevel, maxLevel, new util.TreeMap[Key, Element])
  }

  def destroy(): Unit = {
    log.debug("destroy")
    this.logger.close()
    Utils.deleteFile(this.dirPath + java.io.File.separator + Nursery.LOG_FILENAME)
  }

  def doAdd(rawKey: Array[Byte], value: Value, keyExpireSecs: Int, top: ActorRef): Boolean = {
    log.debug(
      "doAdd, rawKey: %s, value: %s, keyExpireSecs: %d, top: %s"
        .format(Utils.toHexStrings(rawKey), value, keyExpireSecs, top))

    val dbExpireSecs = Settings.getSettings.getInt("goju.expiry_secs", 0)
    val keyValue = if (keyExpireSecs + dbExpireSecs == 0) {
      new KeyValue(rawKey, value, None)
    } else {
      val expireTime = if (dbExpireSecs == 0) {
        Utils.expireTime(keyExpireSecs)
      } else {
        Utils.expireTime(Math.min(keyExpireSecs, dbExpireSecs))
      }
      new KeyValue(rawKey, value, Option(expireTime))
    }
    tree.put(keyValue.key(), keyValue)
    val data = Utils.encodeIndexNode(keyValue)
    logger.write(data)
    doSync()

    this.totalSize += data.length
    doIncMerge(1, top)

    hasRoom(1)
  }

  def doSync(): Unit = {
    val syncStrategy = Settings.getSettings.getInt("goju.sync_strategy", 0)
    this.lastSync = syncStrategy match {
      case 0 =>
        logger.flush()
        System.currentTimeMillis
      case secs if secs > 0 =>
        if ((System.currentTimeMillis - this.lastSync / 1000L) > secs) {
          logger.flush()
          System.currentTimeMillis
        } else {
          this.lastSync
        }
    }
  }

  def lookup(key: Array[Byte]): Option[KeyValue] = {
    log.debug("lookup(Nursery) key: %s".format(key))
    val element = this.tree.get(Key(key))
    element match {
      case kv: KeyValue =>
        Option(kv)
      case _ =>
        None
    }
  }

  def doIncMerge(n: Int, top: ActorRef): Unit = {
    log.debug("doIncMerge, n: %d, top: %s".format(n, top))
    log.debug("doIncMerge, this.step: %d, this.minLevel: %d".format(this.step, this.minLevel))
    if (this.step + n >= (Utils.btreeSize(this.minLevel) / 2)) {
      log.debug("doIncMerge, this.step + n >= (Utils.btreeSize(this.minLevel) / 2)")
      Level.beginIncrementalMerge(top, this.step + n)
      this.mergeDone = this.mergeDone + this.step + n
      this.step = 0
    } else {
      this.step = this.step + n
    }
  }

  def hasRoom(n: Int): Boolean = {
    log.debug(
      "hasRoom: this.tree.size: %d, n: %d, this.minLevel: %d"
        .format(this.tree.size, n, this.minLevel))
    val hasRoom = (this.tree.size + n + 1) < (1 << this.minLevel)
    log.debug("hasRoom: " + hasRoom)
    hasRoom
  }

  def transact(transactionSpecs: List[(TransactionOp, Any)], top: ActorRef): Unit = {
    var size = 0
    val dbExpireSecs = Settings.getSettings.getInt("goju.expiry_secs", 0)
    val ops = transactionSpecs.map {
      case (Delete, key: Array[Byte]) =>
        new KeyValue(key, Constants.TOMBSTONE, Option(Utils.expireTime(dbExpireSecs)))
      case (Put, (key: Array[Byte], value: Value)) =>
        new KeyValue(key, value, Option(Utils.expireTime(dbExpireSecs)))
      case spec => throw new IllegalArgumentException("Unexpected spec: %s".format(spec))
    }

    ops.foreach(op => {
      val data = Utils.encodeIndexNode(op)
      logger.write(data)
      size += data.length
    })
    doSync()

    ops.foreach(op => {
      tree.put(op.key(), op)
    })
    this.totalSize += size
    doIncMerge(ops.length, top)
  }

  def doLevelFold(foldWorkerPid: ActorRef, ref: ActorRef, range: KeyRange): Unit = {
    log.debug(
      "doLevelFold, foldWorkerPid: %s, ref: %s, range: %s".format(foldWorkerPid, ref, range))
    val (lastKey, count) = this.tree.foldLeft(None: Option[Key], range.limit) {
      case ((lastKey, count), (k: Key, e: Element)) =>
        if (count == 0) {
          (lastKey, count)
        } else {
          if (range.keyInFromRange(k) && range.keyInToRange(k) && !e.expired()) {
            log.debug("doLevelFold, KeyValue found. key: %s".format(k))
            foldWorkerPid ! (LevelResult, ref, e)
            if (e.tombstoned()) {
              (Option(e.key()), count)
            } else {
              (Option(e.key()), count - 1)
            }
          } else {
            (lastKey, count)
          }
        }
    }
    if (lastKey.isDefined && (count == 0)) {
      log.debug("doLevelFold, got to limit. lastKey: %s".format(lastKey.get))
      foldWorkerPid ! (LevelLimit, ref, lastKey.get)
    } else {
      log.debug("doLevelFold, finished")
      foldWorkerPid ! (LevelDone, ref)
    }
  }
}
