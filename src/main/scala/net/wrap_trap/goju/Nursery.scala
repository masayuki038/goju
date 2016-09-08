package net.wrap_trap.goju

import collection.JavaConversions._
import java.io.{FileOutputStream, File, FileWriter}
import java.util.TreeMap

import akka.actor.ActorRef
import com.typesafe.config.Config
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.{KeyValue, Element}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Nursery {
  private val LOG_FILENAME = "nursery.log"
  private val DATA_FILENAME = "nursery.data"

  def newNursery(dirPath: String, minLevel: Int, maxLevel: Int): Nursery = {
    Utils.ensureExpiry
    new Nursery(dirPath, minLevel, maxLevel)
  }

  def flush(nursery: Nursery, top: ActorRef): Nursery = {
    val logFile = new File(nursery.dirPath + java.io.File.pathSeparator + Nursery.LOG_FILENAME)
    finish(nursery, logFile, top)
    if(logFile.exists) {
      throw new IllegalStateException("Failed to delete log file")
    }
    newNursery(nursery.dirPath, nursery.minLevel, nursery.maxLevel)
  }

  def add(key: Array[Byte], value: Value, nursery: Nursery, top: ActorRef): Unit = {
    add(key, value, 0, nursery, top)
  }

  def add(key: Array[Byte], value: Value, keyExpireSecs: Int, nursery: Nursery, top: ActorRef): Unit = {
    if(nursery.doAdd(key, value, keyExpireSecs, top)) {
      flush(nursery, top)
    }
  }

  def recover(dirPath: String, topLevel: ActorRef, minLevel: Int, maxLevel: Int): Nursery = {
    if(minLevel > maxLevel) {
      throw new IllegalArgumentException(s"""$minLevel(minLevel) > $maxLevel(maxLevel)""")
    }
    Utils.ensureExpiry

    val logFile = new File(dirPath + java.io.File.separator + LOG_FILENAME)
    if(logFile.exists()) {
      doRecover(logFile, topLevel, minLevel, maxLevel)
    }
    new Nursery(dirPath, minLevel, maxLevel)
  }

  def doRecover(logFile: File, topLevel: ActorRef, minLevel: Int, maxLevel: Int): Unit = {
    val nursery = readNurseryFromLog(logFile, minLevel, maxLevel)
    finish(nursery, logFile, topLevel)
    if(logFile.exists) {
      throw new IllegalStateException("Failed to delete log file in recover")
    }
  }

  private def finish(nursery: Nursery, logFile: File, topLevel: ActorRef): Unit = {
    Utils.ensureExpiry

    if(nursery.tree.size > 0) {
      val writer = Writer.open(nursery.dirPath + java.io.File.separator + DATA_FILENAME)
      for(e <- nursery.tree.values) {
        Writer.add(writer, e)
      }
      Writer.close(writer)
    }
    // TODO inject & merge

    nursery.destroy()
  }

  private def readNurseryFromLog(logFile: File, minLevel: Int, maxLevel: Int): Nursery = {
    val logBinary = java.nio.file.Files.readAllBytes(logFile.toPath)
    val recovered = Utils.decodeCRCData(logBinary, List.empty[Array[Byte]], List.empty[Element])
    val tree = new TreeMap[Array[Byte], Element]
    for(e <- recovered) {if(!e.tombstoned) {tree.put(e.key.bytes, e)}}
    new Nursery(logFile.getParent, minLevel, maxLevel, tree)
  }
}

class Nursery(val dirPath: String, val minLevel: Int, val maxLevel: Int, val tree: TreeMap[Array[Byte], Element]) {
  def this(dirPath: String, minLevel: Int, maxLevel: Int) = {
    this(dirPath, minLevel, maxLevel, new TreeMap[Array[Byte], Element])
  }
  val logger = new FileOutputStream(dirPath + java.io.File.separator + Nursery.LOG_FILENAME, true)
  var lastSync = System.currentTimeMillis
  var count = 0

  def destroy() = {
    logger.close
    new File(dirPath + java.io.File.separator + Nursery.LOG_FILENAME).delete()
  }

  def doAdd(key: Array[Byte], value: Value, keyExpireSecs: Int, top: ActorRef): Boolean = {
    val dbExpireSecs = Settings.getSettings().getInt("goju.expiry_secs", 0)
    val keyValue = (keyExpireSecs + dbExpireSecs == 0) match {
      case true => {
        new KeyValue(key, value, None)
      }
      case _ => {
        val expireTime = (dbExpireSecs == 0) match {
          case true => Utils.expireTime(dbExpireSecs)
          case _ => Utils.expireTime(Math.min(keyExpireSecs, dbExpireSecs))
        }
        new KeyValue(key, value, Option(expireTime))
      }
    }
    tree.put(key, keyValue)
    val data = Utils.encodeIndexNode(keyValue)
    logger.write(data)
    doSync()
    hasRoom(1)
  }

  def doSync() = {
    val syncStrategy = Settings.getSettings().getInt("goju.sync_strategy", 0)
    this.lastSync = syncStrategy match {
      case 0 => {
        logger.flush()
        System.currentTimeMillis
      }
      case secs if secs > 0 => {
        if((System.currentTimeMillis - this.lastSync / 1000L) > secs) {
          logger.flush()
          System.currentTimeMillis
        } else {
          this.lastSync
        }
      }
    }
  }

  def doIncMerge() = {
    // TODO hanoidb_level:begin_incremental_merge
  }

  def hasRoom(n: Int): Boolean = {
    (count + n + 1) < (1 << this.minLevel)
  }
}
