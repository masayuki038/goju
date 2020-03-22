package net.wrap_trap.goju

import java.nio.charset.Charset

import akka.actor.ActorRef
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.KeyValue
import net.wrap_trap.goju.element.Element

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Constants {
  type Value = Any

  val TOMBSTONE = Utils.toBytes("deleted")
  val FILE_FORMAT = "HAN2"
  val FIRST_BLOCK_POS: Long = FILE_FORMAT.getBytes.size
  val FOLD_CHUNK_SIZE = 100;
  val TOP_LEVEL = 8

  val TAG_KV_DATA = 0x80.asInstanceOf[Byte]
  val TAG_DELETED = 0x81.asInstanceOf[Byte]
  val TAG_POSLEN = 0x82.asInstanceOf[Byte]
  val TAG_TRANSACT = 0x83.asInstanceOf[Byte]
  val TAG_KV_DATA2 = 0x84.asInstanceOf[Byte]
  val TAG_DELETED2 = 0x85.asInstanceOf[Byte]
  val TAG_END = 0xff.asInstanceOf[Byte]

  val SIZE_OF_ENTRY_TYPE = 1
  val SIZE_OF_KEYSIZE = 4
  val SIZE_OF_TIMESTAMP = 4
  val SIZE_OF_POS = 8
  val SIZE_OF_LEN = 4

  val COMPRESS_PLAIN = 0x00.asInstanceOf[Byte]

  val MERGE_STRATEGY_FAST = 1
  val MERGE_STRATEGY_PREDICTABLE = 2
}

sealed abstract class TransactionOp
case object Delete extends TransactionOp
case object Put extends TransactionOp

sealed abstract class MergeOp
case object Step extends MergeOp
case object MergeDone extends MergeOp

sealed abstract class GojuOp
case object Get extends GojuOp
case object Transact extends GojuOp

sealed abstract class RangeOp
case object Start extends RangeOp

sealed abstract class RangeType
case object BlockingRange extends RangeType
case object SnapshotRange extends RangeType

sealed abstract class LevelOp
case object Query extends LevelOp
case class Lookup(key: Array[Byte], from: Option[ActorRef]) extends LevelOp
case class LookupAsync(key: Array[Byte], f: (Option[Value] => Unit)) extends LevelOp
case object Inject extends LevelOp
case object BeginIncrementalMerge extends LevelOp
case object AwaitIncrementalMerge extends LevelOp
case object UnmergedCount extends LevelOp
case object SetMaxLevel extends LevelOp
case object Close extends LevelOp
case object Destroy extends LevelOp
case class InitSnapshotRangeFold(gojuActor: Option[ActorRef], workerPid: ActorRef, range: KeyRange, refList: List[String]) extends LevelOp
case class InitBlockingRangeFold(gojuActor: Option[ActorRef], workerPid: ActorRef, range: KeyRange, refList: List[String]) extends LevelOp
case object LevelResult extends LevelOp
case class LevelResults(pid: ActorRef, kvs: List[Element]) extends LevelOp
case object RangeFoldDone extends LevelOp
case object LevelLimit extends LevelOp
case object LevelDone extends LevelOp
case object BottomLevel extends LevelOp

case object StepLevel extends LevelOp
case object StepDone extends LevelOp
case object StepOk extends LevelOp

sealed abstract class LookupResponse
case object NotFound extends LookupResponse
case object Found extends LookupResponse
case object Delegate extends LookupResponse

sealed abstract class FoldWorkerOp
case class Initialize(val refList: List[String]) extends FoldWorkerOp
case class Prefix(val refList: List[String]) extends FoldWorkerOp

case object Done
case object Limit

sealed abstract class FoldStatus
case object Stop extends FoldStatus
case object Stopped extends FoldStatus
case object Continue extends FoldStatus
case object Ok extends FoldStatus
case object FoldLimit extends FoldStatus
case object FoldResult extends FoldStatus
case object FoldDone extends FoldStatus

case object WaitForAllChildrenStopped
case object StopChild