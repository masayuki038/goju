package net.wrap_trap.goju

import java.io.File
import java.nio.file.{Paths, Files}

import akka.actor._
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.Element
import net.wrap_trap.goju.element.KeyValue
import org.hashids.Hashids
import org.hashids.syntax._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Level extends PlainRpc {
  val log = Logger(LoggerFactory.getLogger(Level.getClass))
  val callTimeout = Settings.getSettings().getInt("goju.level.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  def open(dirPath: String, level: Int, owner: Option[ActorRef]): ActorRef = {
    Utils.ensureExpiry
    Utils.getActorSystem.actorOf(Props(classOf[Level], dirPath, level, owner))
  }

  def level(ref: ActorRef): Int = {
    call(ref, Query) match {
      case ret: Int => ret
    }
  }

  def lookup(ref: ActorRef, key: Array[Byte]): Option[KeyValue] = {
    call(ref, (Lookup, key)) match {
      case NotFound => None
      case kv: KeyValue => Option(kv)
    }
  }

  def lookup(ref: ActorRef, key: Array[Byte], f: Option[Value] => Unit): Unit = {
    cast(ref, (Lookup, key, f))
  }

  def inject(ref: ActorRef, filename: String): Any = {
    call(ref, (Inject, filename))
  }

  def beginIncrementalMerge(ref: ActorRef, stepSize: Int): Any = {
    log.debug("beginIncrementalMerge: stepSize: %d".format(stepSize))
    call(ref, (BeginIncrementalMerge, stepSize))
  }

  def awaitIncrementalMerge(ref: ActorRef): Any = {
    call(ref, AwaitIncrementalMerge)
  }

  def unmergedCount(ref: ActorRef): Int = {
    call(ref, UnmergedCount) match  {
      case ret: Int => ret
    }
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
    log.debug("snapshotRange, ref: %s, foldWorkerRef: %s, keyRange: %s".format(ref, foldWorkerRef, keyRange))
    val folders = call(ref, (InitSnapshotRangeFold, foldWorkerRef, keyRange, List.empty[String]))
      .asInstanceOf[List[String]]
    foldWorkerRef ! (Initialize, folders)
  }

  def blockingRange(ref: ActorRef, foldWorkerRef: ActorRef, keyRange: KeyRange): Unit = {
    log.debug("blockingRange, ref: %s, foldWorkerRef: %s, keyRange.fromKey: %s, keyRange.toKey: %s"
      .format(ref, foldWorkerRef, keyRange.fromKey, keyRange.toKey))
    val folders = call(ref, (InitBlockingRangeFold, foldWorkerRef, keyRange, List.empty[String]))
      .asInstanceOf[List[String]]
    foldWorkerRef ! (Initialize, folders)
  }
}

class Level(val dirPath: String, val level: Int, val owner: Option[ActorRef]) extends Actor with PlainRpc {
  val log = Logger(LoggerFactory.getLogger(Level.getClass))
  implicit val hashids = Hashids.reference(this.hashCode.toString)

  var aReader: Option[RandomReader] = None
  var bReader: Option[RandomReader] = None
  var cReader: Option[RandomReader] = None

  var next: Option[ActorRef] = None

  var wip: Option[Int] = None
  var workDone = 0
  var maxLevel = 0

  var mergePid: Option[ActorRef] = None
  var stepMergeRef: Option[ActorRef] = None
  var stepNextRef: Option[ActorRef] = None
  var stepCaller: Option[ActorRef] = None
  var injectDoneRef: Option[ActorRef] = None

  var folding: List[ActorRef] = List.empty[ActorRef]

  override def preStart(): Unit = {
    initialize()
  }

  private def initialize(): Unit = {
    Utils.ensureExpiry
    val aFileName = filename("A")
    val bFileName = filename("B")
    val cFileName = filename("C")
    val mFileName = filename("M")

    Utils.deleteFile(filename("X"))
    Utils.deleteFile(filename("AF"))
    Utils.deleteFile(filename("BF"))
    Utils.deleteFile(filename("CF"))

    new File(mFileName).exists match {
      case true => {
        Utils.deleteFile(aFileName)
        Utils.deleteFile(bFileName)
        Utils.renameFile(mFileName, aFileName)

        this.aReader = Option(RandomReader.open(aFileName))

        new File(cFileName).exists match {
          case true => {
            Utils.renameFile(cFileName, bFileName)
            this.bReader = Option(RandomReader.open(bFileName))
            checkBeginMergeThenLoop0()
          }
          case false => {
            mainLoop()
          }
        }
      }
      case false => {
        new File(bFileName).exists match {
          case true => {
            this.aReader = Option(RandomReader.open(aFileName))
            this.bReader = Option(RandomReader.open(bFileName))
            new File(cFileName).exists match {
              case true =>
                this.cReader = Option(RandomReader.open(cFileName))
            }
          }
          case false => {
            new File(cFileName).exists match {
              case true =>
                throw new IllegalStateException("Conflict: bFileName doesn't exist but cFileName exists")
              case false => {
                new File(aFileName).exists match {
                  case true => {
                    this.aReader = Option(RandomReader.open(aFileName))
                  }
                  case false => // do nothing
                }
                mainLoop()
              }
            }
          }
        }
      }
    }
  }

  private def mainLoop(): Unit = {
    // loop
  }

  private def checkBeginMergeThenLoop0(): Unit = {
    log.debug("checkBeginMergeThenLoop0")
    if(aReader.isDefined && bReader.isDefined && mergePid.isEmpty) {
      val mergeRef = beginMerge()
      val watcher = context.watch(mergeRef)

      val progress = this.cReader match {
        case Some(r) => 2 * Utils.btreeSize(this.level)
        case _ => Utils.btreeSize(this.level)
      }
      mergeRef ! (Step, (self, watcher), progress)

      this.mergePid = Option(mergeRef)
      this.stepMergeRef = Option(watcher)
      this.wip = Option(progress)
      this.workDone = 0
      mainLoop()
    } else {
      checkBeginMergeThenLoop()
    }
  }

  private def checkBeginMergeThenLoop(): Unit = {
    log.debug("checkBeginMergeThenLoop, %s, %s, %s".format(aReader, bReader, mergePid))
    if(aReader.isDefined && bReader.isDefined && mergePid.isEmpty) {
      log.debug("checkBeginMergeThenLoop, aReader.isDefined && bReader.isDefined && mergePid.isEmpty")
      val mergeRef = beginMerge()
      this.mergePid = Option(mergeRef)
      this.workDone = 0
    } else {
      log.debug("checkBeginMergeThenLoop, don't call beginMerge")
    }
    mainLoop()
  }

  private def beginMerge(): ActorRef = {
    log.debug("beginMerge")
    val aFileName = filename("A")
    val bFileName = filename("B")
    val xFileName = filename("X")

    Utils.deleteFile(xFileName)

    context.actorOf(
      Props(classOf[Merge], self, aFileName, bFileName, xFileName, Utils.btreeSize(this.level + 1), next.isEmpty)
    )
  }

  private def filename(prefix: String): String = {
    "%s%s%s-%d.data".format(dirPath, File.separator, prefix, this.level)
  }

  def receive = {
    case (PlainRpcProtocol.call, (Lookup, key: Array[Byte])) => {
      doLookup(key, List(this.cReader, this.bReader, this.aReader), this.next) match {
        case NotFound => sendReply(sender(), NotFound)
        case (Found, kv) => sendReply(sender(), kv)
        case (Delegate, pid: ActorRef) => pid ! (PlainRpcProtocol.call, (Lookup, key))
      }
    }
    case (PlainRpcProtocol.cast, (Lookup, key: Array[Byte], f: (Option[Value] => Unit))) => {
      doLookup(key, List(this.cReader, this.bReader, this.aReader), this.next) match {
        case NotFound => f(None)
        case (Found, value: Option[Value]) => f(value)
        case (Delegate, pid: ActorRef) => pid ! (PlainRpcProtocol.call, (Lookup, key, f))
      }
    }
    case (PlainRpcProtocol.call, (Inject, fileName: String)) => {
      log.debug("receive Inject: fileName: %s".format(fileName))
      val (toFileName, pos) = (this.aReader, this.bReader) match {
        case (None, None) => (filename("A"), 1)
        case (_, None) => (filename("B"), 2)
        case (_, _) => (filename("C"), 3)
      }
      log.debug("receive Inject, pos: " + pos)
      Utils.renameFile(fileName, toFileName)

      val newReader = Option(RandomReader.open(toFileName))
      pos match {
        case 1 => this.aReader = newReader
        case 2 => {
          this.bReader = newReader
          checkBeginMergeThenLoop()
        }
        case 3 => this.cReader = newReader
      }
      sendReply(sender(), true)
    }
    case (PlainRpcProtocol.call, UnmergedCount) => sendReply(sender(), totalUnmerged)
    case (PlainRpcProtocol.cast, (SetMaxLevel, max: Int)) => {
      if(next.isDefined) {
        Level.setMaxLevel(next.get, max)
      }
      this.maxLevel = max
    }
    case (PlainRpcProtocol.call, (BeginIncrementalMerge, stepSize: Int))
      if (this.stepMergeRef.isEmpty && this.stepNextRef.isEmpty) => {
      log.debug("receive BeginIncrementalMerge: stepSize: %d".format(stepSize))
      sendReply(sender(), true)
      doStep(None, 0, stepSize)
    }
    case (PlainRpcProtocol.call, AwaitIncrementalMerge)
      if (this.stepMergeRef.isEmpty && this.stepNextRef.isEmpty) => {
      sendReply(sender(), true)
    }
    case (PlainRpcProtocol.call, (StepLevel, doneWork: Int, stepSize: Int))
      if (this.stepMergeRef.isEmpty && this.stepCaller.isEmpty && this.stepNextRef.isEmpty) => {
      doStep(Option(sender()), doneWork, stepSize)
    }
    case (PlainRpcProtocol.call, Query) => {
      sendReply(sender(), this.level)
    }
    case (monitorRef: ActorRef, StepDone) if (this.stepMergeRef.isDefined && monitorRef == this.stepMergeRef.get) => {
      context.unwatch(monitorRef)
      this.wip match {
        case Some(w) => {
          this.workDone += w
          this.wip = Option(0)
        }
      }
      this.stepNextRef match {
        case Some(_) => // do nothing
        case None => replyStepOk()
      }
      this.stepMergeRef = None
    }
    case e: Terminated if (this.stepMergeRef.isDefined && e.actor == this.stepMergeRef.get) => {
      this.stepNextRef match {
        case Some(_) => // do nothing
        case None => replyStepOk()
      }
      this.stepMergeRef = None
      this.wip = Option(0)
    }
    case (PlainRpcProtocol.reply, (StepOk, monitorRef: ActorRef))
      if((this.stepNextRef.isDefined && monitorRef == this.stepNextRef.get) && this.stepMergeRef.isEmpty) => {
      context.unwatch(monitorRef)
      this.stepNextRef = None
    }
    case (PlainRpcProtocol.call, Close) => {
      closeIfDefined(this.aReader)
      closeIfDefined(this.bReader)
      closeIfDefined(this.cReader)
      val list = this.mergePid match {
        case Some(s) => s :: folding
        case _ => folding
      }
      list.foreach(p => context.stop(p))
      this.folding = List.empty[ActorRef]
      this.stepMergeRef = None

      this.next match {
        case Some(n) => Level.close(n)
        case _ => // do nothing
      }
      sendReply(sender(), true)
    }
    case (PlainRpcProtocol.call, Destroy) => {
      destroyIfDefined(this.aReader)
      destroyIfDefined(this.bReader)
      destroyIfDefined(this.cReader)
      val list = this.mergePid match {
        case Some(s) => s :: folding
        case _ => folding
      }
      list.foreach(p => context.stop(p))

      this.next match {
        case Some(n) => Level.destroy(n)
        case _ => // do nothing
      }
      sendReply(sender(), true)
      context.stop(self)
    }
    case (PlainRpcProtocol.call, (InitSnapshotRangeFold, workerPid: ActorRef, range: KeyRange, refList: List[ActorRef]))
      if(this.folding.isEmpty) => {
      log.debug("receive InitSnapshotRangeFold, workerPid: %s, range: %s, list: %s".format(workerPid, range, refList))
      val (nextList, foldingPids) = (this.aReader, this.bReader, this.cReader) match {
        case (None, None, None) => {
          log.debug("InitSnapshotRangeFold, case (None, None None)")
          (refList, List.empty[ActorRef])
        }
        case (_, None, None) => {
          log.debug("InitSnapshotRangeFold, case (_, None None)")
          Files.createLink(Paths.get(filename("A")), Paths.get(filename("AF")))
          val pid0 = startRangeFold(filename("AF"), workerPid, range)
          (pid0 :: refList, List(pid0))
        }
        case (_, _, None) => {
          log.debug("InitSnapshotRangeFold, case (_, _, None)")
          Files.createLink(Paths.get(filename("A")), Paths.get(filename("AF")))
          val pidA = startRangeFold(filename("AF"), workerPid, range)
          Files.createLink(Paths.get(filename("B")), Paths.get(filename("BF")))
          val pidB = startRangeFold(filename("BF"), workerPid, range)
          (List(pidA, pidB) ::: refList, List(pidB, pidA))
        }
        case (_, _, _) => {
          log.debug("InitSnapshotRangeFold, case (_, _, _)")
          Files.createLink(Paths.get(filename("A")), Paths.get(filename("AF")))
          val pidA = startRangeFold(filename("AF"), workerPid, range)
          Files.createLink(Paths.get(filename("B")), Paths.get(filename("BF")))
          val pidB = startRangeFold(filename("BF"), workerPid, range)
          Files.createLink(Paths.get(filename("C")), Paths.get(filename("CF")))
          val pidC = startRangeFold(filename("CF"), workerPid, range)
          (List(pidA, pidB, pidC) ::: refList, List(pidC, pidB, pidA))
        }
      }

      log.debug("InitSnapshotRangeFold, this.next: %s".format(this.next))
      this.next match {
        case Some(n) =>sender() ! (PlainRpcProtocol.call, (InitSnapshotRangeFold, workerPid, range, nextList))
        case _ => sendReply(sender(), nextList.reverse)
      }
      this.folding = foldingPids
    }
    case (RangeFoldDone, pid: ActorRef, filePath: String) => {
      Utils.deleteFile(filePath)
      this.folding = this.folding.filter(p => p != pid)
    }
    case (PlainRpcProtocol.call, (InitBlockingRangeFold, workerPid: ActorRef, range: KeyRange, refList: List[String])) => {
      log.debug("receive InitBlockingRangeFold, workerPid: %s, range: %d, list: %s".format(workerPid, range, refList))
      val newRefList = (this.aReader, this.bReader, this.cReader) match {
        case (None, None, None) => refList
        case (Some(a), None, None) => {
          val aRef = System.nanoTime.hashid
          doRangeFold(a, workerPid, aRef, range)
          aRef :: refList
        }
        case (Some(a), Some(b), None) => {
          val aRef = System.nanoTime.hashid
          doRangeFold(a, workerPid, aRef, range)

          val bRef = System.nanoTime.hashid
          doRangeFold(b, workerPid, bRef, range)

          List(aRef, bRef) :: refList
        }
        case (Some(a), Some(b), Some(c)) => {
          val aRef = System.nanoTime.hashid
          doRangeFold(a, workerPid, aRef, range)

          val bRef = System.nanoTime.hashid
          doRangeFold(b, workerPid, bRef, range)

          val cRef = System.nanoTime.hashid
          doRangeFold(b, workerPid, cRef, range)

          List(aRef, bRef, cRef) :: refList
        }
      }
      this.next match {
        case Some(n) =>sender() ! (PlainRpcProtocol.call, (InitBlockingRangeFold, workerPid, range, newRefList))
        case _ => sendReply(sender(), (Ok, newRefList.reverse))
      }
    }
    case (PlainRpcProtocol.cast, (MergeDone, 0, outFileName: String)) => {
      log.debug("receive (MergeDone, count: 0, outFileName: %s)".format(outFileName))

      Utils.deleteFile(outFileName)
      closeAndDeleteAAndB()

      this.mergePid = None
      this.cReader match {
        case None => // do nothing
        case Some(reader) => {
          reader.close
          val aFileName = filename("A")
          Utils.renameFile(filename("C"), aFileName)
          this.aReader = Option(RandomReader.open(aFileName))
          this.cReader = None
        }
      }
    }
    case  (PlainRpcProtocol.cast, (MergeDone, count: Int, outFileName: String))
      if Utils.btreeSize(this.level) >= count && this.cReader.isEmpty && this.next.isEmpty => {
      log.debug("receive (MergeDone, count: %d, outFileName: %s)".format(count, outFileName))
      log.debug("if Utils.btreeSize(this.level) >= count && this.cReader.isEmpty && this.next.isEmpty")

      val mFileName = filename("M")
      Utils.renameFile(outFileName, mFileName)
      closeAndDeleteAAndB()

      val aFileName = filename("A")
      Utils.renameFile(mFileName, aFileName)
      this.aReader = Option(RandomReader.open(aFileName))

      this.mergePid = None
      this.cReader match {
        case None => this.bReader = None
        case Some(reader) => {
          reader.close()
          val bFileName = filename("B")
          Utils.renameFile(filename("C"), bFileName)
          this.bReader = Option(RandomReader.open(bFileName))
          checkBeginMergeThenLoop()
        }
      }
    }
    case  (PlainRpcProtocol.cast, (MergeDone, count: Int, outFileName: String)) => {
      log.debug("receive (MergeDone, count: %d, outFileName: %s)".format(count, outFileName))
      if(next.isEmpty) {
        val level = Level.open(this.dirPath, this.level + 1, this.owner)
        this.owner.foreach{ o => o ! (BottomLevel, this.level + 1)}
        this.next = Option(level)
        this.maxLevel = this.level + 1
      }
      this.next match {
        case Some(n) => {
          val mRef = sendCall(n, this.context, (Inject, outFileName))
          this.injectDoneRef = Option(mRef)
          this.mergePid = None
        }
      }
    }
    case (PlainRpcProtocol.reply, (mRef: ActorRef, Ok)) => {
      this.context.unwatch(mRef)
      closeAndDeleteAAndB()
      this.cReader match {
        case None => // do nothing
        case Some(reader) => {
          Utils.renameFile(filename("C"), filename("A"))
          this.aReader = this.cReader
          this.bReader = None
          this.cReader = None
        }
      }
    }
    case Terminated(mRef) if mRef == this.injectDoneRef => {
      throw new IllegalStateException("injectDoneRef has down")
    }
  }

  private def closeAndDeleteAAndB(): Unit = {
    val aFileName = filename("A")
    val bFileName = filename("B")

    this.aReader match {
      case Some(reader) => reader.close()
      case _ => // do nohting
    }

    this.bReader match {
      case Some(reader) => reader.close()
      case _ => // do nohting
    }

    Utils.deleteFile(aFileName)
    Utils.deleteFile(bFileName)

    this.aReader = None
    this.bReader = None
  }

  private def doRangeFold(reader: RandomReader, workerPid: ActorRef, ref: String, range: KeyRange): Unit = {
    val (_, values) = reader.rangeFold((e, acc0) => {
        workerPid ! (LevelResult, ref, e)
        acc0
      },
      (100, List.empty[Element]),
      range
    )

    values.length match {
      case range.limit => workerPid ! (LevelLimit, ref)
      case _ => workerPid ! (LevelDone, ref)
    }
  }

  private def startRangeFold(path: String, workerPid: ActorRef, range: KeyRange): ActorRef = {
    context.actorOf(Props(classOf[RangeFolder], path, workerPid, sender(), range))
  }

  private def destroyIfDefined(reader: Option[RandomReader]): Unit = {
    reader match {
      case Some(r) => r.destroy
      case _ => // do nothing
    }
  }

  private def closeIfDefined(reader: Option[RandomReader]): Unit = {
    reader match {
      case Some(r) => r.close
      case _ => // do nothing
    }
  }

  private def doStep(stepFrom: Option[ActorRef], previousWork: Int, stepSize: Int): Unit = {
    log.debug("doStep: stepFrom: %s, previousWork: %d, stepSize: %d".format(stepFrom, previousWork, stepSize))
    var workLeftHere = 0
    if(this.bReader.isDefined && this.mergePid.isDefined) {
      workLeftHere = Math.max(0, (2 * Utils.btreeSize(this.level)) - this.workDone)
    }
    val workUnit = stepSize
    val maxLevel = Math.max(this.maxLevel, this.level)
    val depth = maxLevel - Constants.TOP_LEVEL + 1
    val totalWork = depth * workDone
    val workUnitsLeft = Math.max(0, totalWork - previousWork)

    val workToDoHere = Settings.getSettings().getInt("merge.strategy", 1) match {
      case Constants.MERGE_STRATEGY_FAST => Math.min(workLeftHere, workUnit)
      case Constants.MERGE_STRATEGY_PREDICTABLE => {
        if(workLeftHere < depth * workUnit) {
          Math.min(workLeftHere, workUnit)
        } else {
          Math.min(workLeftHere, workUnitsLeft)
        }
      }
    }

    log.debug("doStep: workUnit: %d, maxLevel: %d, depth: %d, totalWork: %d, workUnitsLeft: %d, workToDoHere: %d"
      .format(workUnit, maxLevel, depth, totalWork, workUnitsLeft, workToDoHere))

    val workIncludingHere = previousWork + workToDoHere

    log.debug("doStep: workIncludingHere: %d".format(workIncludingHere))

    val delegateRef = this.next match {
      case None => None
      case Some(n) => Option(sendCall(n, context, (StepLevel, workIncludingHere, stepSize)))
    }

    log.debug("doStep: delegateRef: %s".format(delegateRef))

    val mergeRef = (workToDoHere > 0) match {
      case true => {
        this.mergePid match {
          case Some(pid) => {
            val monitor = context.watch(pid)
            pid ! (Step, workToDoHere)
            Option(monitor)
          }
        }
      }
      case false => None
    }

    log.debug("doStep: mergeRef: %s".format(mergeRef))

    if(delegateRef.isEmpty && mergeRef.isEmpty) {
      log.debug("doStep: delegateRef.isEmpty && mergeRef.isEmpty")
      this.stepCaller = stepFrom
      replyStepOk()
    } else {
      log.debug("doStep: delegateRef.isDefined || mergeRef.isDefined")
      this.stepNextRef = delegateRef
      this.stepCaller = stepFrom
      this.stepMergeRef = mergeRef
      this.wip = Option(workToDoHere)
    }
  }

  private def replyStepOk(): Unit = {
    this.stepCaller match {
      case Some(caller) => sendReply(caller, StepOk)
      case None => // do nothing
    }
    this.stepCaller = None
  }

  private def totalUnmerged(): Int = {
    val files = this.bReader match {
      case Some(_) => 2
      case _ => 0
    }
    files * Utils.btreeSize(this.level)
  }

  private def doLookup(key: Array[Byte], list: List[Option[RandomReader]], next: Option[ActorRef]): Any = {
    list match {
      case List() => next match {
        case Some(pid) => (Delegate, pid)
        case _ => NotFound
      }
      case List(None, _*) => doLookup(key, list.tail, next)
      case List(Some(reader), _*) => {
        reader.lookup(key) match {
          case Some(kv) => (Found, kv)
          case None => {
            // TODO if value is tombstoned, stopping to call doLoockup recursively and return None
            doLookup(key, list.tail, next)
          }
        }
      }
    }
  }
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
case object LevelResult extends LevelOp
case object LevelResults extends LevelOp
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
case object Initialize extends FoldWorkerOp
case object Prefix extends FoldWorkerOp