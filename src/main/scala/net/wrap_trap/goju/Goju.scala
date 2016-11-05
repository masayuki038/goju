package net.wrap_trap.goju

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