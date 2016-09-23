package net.wrap_trap.goju

import akka.actor.{Actor, ActorRef}
import net.wrap_trap.goju.element.Element

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Merge {
  def start(): Unit = {
    // TODO
  }

}

class Merge(val aPath: String, val bPath: String, val cPath: String, val size: Int, val isLastLevel: Boolean) extends Actor {
  val aReader = SequentialReader.open(aPath)
  val bReader = SequentialReader.open(bPath)
  val out = Writer.open(cPath)
  var n = 0
  var aKVs:Option[List[Element]] = None
  var bKVs:Option[List[Element]] = None

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
    case (Step, howMany: Int) => {
      this.n += howMany
      scan()
    }
      // TODO 'system' and 'exit'
  }

  // Expect to call this method from "merge" only
  // Therefore, don't call back merging states to other actor
  private def scan(): Unit = {
    val a = aKVs.get
    val b = bKVs.get

    if(n < 1 && a.nonEmpty && b.nonEmpty) {
      // stop scan and do nothing
      return
    }

    if(a.isEmpty) {
      aReader.nextNode() match {
        case Some(a2) => {
          this.aKVs = Option(a2)
          scan()
        }
        case None => {
          aReader.close()
          // scan_only
        }
      }
    }

    if(b.isEmpty) {
      bReader.nextNode() match {
        case Some(b2) => {
          this.bKVs = Option(b2)
          scan()
        }
        case None => {
          bReader.close()
          // scan_only
        }
      }
    }

  }
}

sealed abstract class MergeOp
case object Step extends MergeOp
case object SystemMerge extends MergeOp
case object Exit extends MergeOp
