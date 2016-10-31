package net.wrap_trap.goju

import akka.actor._
import akka.util.Timeout
import net.wrap_trap.goju.Constants.Value
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

  def close(ref: ActorRef): Unit = {
    try {
      call(ref, Close)
    } catch {
      case ignore => {}
    }
  }

  def destroy(ref: ActorRef): Unit = {
    try {
      call(ref, Destroy)
    } catch {
      case ignore => {}
    }
  }

  def get(ref: ActorRef, key: Array[Byte]): Value = {
    call(ref, (Get, key))
  }

  def lookup(ref: ActorRef, key: Array[Byte]): Value = {
    get(ref, key)
  }

  def delete(ref: ActorRef, key: Array[Byte]): Unit = {
    call(ref, (Delete, key))
  }

  def put(ref: ActorRef, key: Array[Byte], value: Value): Unit = {
    call(ref, (Put, key, value))
  }

  def transact(ref: ActorRef, transactionSpecs: List[(TransactionOp, Any)]): Unit = {
    call(ref, (Transact, transactionSpecs))
  }

//  def fold(ref: ActorRef, func: (Array[Byte], Value, (Int, List[Value])) => (Int, List[Value]), acc0: (Int, List[Value])): List[Value] = {
//    foldRange(ref, func, acc0, KeyRange(new Key(Array.empty[Byte]), true, None, true, Integer.MAX_VALUE))
//  }

//  def foldRange(ref: ActorRef,
//                func: (Array[Byte], Value, (Int, List[Value])) => (Int, List[Value]),
//                acc0: (Int, List[Value]),
//                range: KeyRange): List[Value] = {
//    val rangeType = range.limit < 10 match {
//      case true => BlockingRange
//      case false => SnapshotRange
//    }
//
//  }
}

class Goju(val dirPath: String) {

}

sealed abstract class GojuOp
case object Get extends GojuOp
case object Transact extends GojuOp

sealed abstract class RangeType
case object BlockingRange extends RangeType
case object SnapshotRange extends RangeType