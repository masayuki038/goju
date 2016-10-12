package net.wrap_trap.goju

import akka.actor.{Actor, Props, ActorContext, ActorRef}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Level extends PlainRpc {
  val log = Logger(LoggerFactory.getLogger(Level.getClass))

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
  def receive = {}
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