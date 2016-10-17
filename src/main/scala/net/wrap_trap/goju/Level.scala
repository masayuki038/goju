package net.wrap_trap.goju

import java.io.File

import akka.actor._
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import net.wrap_trap.goju.Constants.Value
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

  def lookup(ref: ActorRef, key: Array[Byte], f: Option[Value] => Unit): Unit = {
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

  var aReader: Option[RandomReader] = None
  var bReader: Option[RandomReader] = None
  var cReader: Option[RandomReader] = None

  var next: Option[ActorRef] = None

  var wip: Option[Int] = None
  var workDone = 0
  var maxLevel = 0

  var stepMergeRef: Option[ActorRef] = None
  var stepNextRef: Option[ActorRef] = None
  var stepCaller: Option[ActorRef] = None

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
    if(aReader.isDefined && bReader.isDefined && stepMergeRef.isEmpty) {
      val merger = beginMerge()
      val watcher = context.watch(merger)

      val progress = this.cReader match {
        case Some(r) => 2 * Utils.btreeSize(this.level)
        case _ => Utils.btreeSize(this.level)
      }
      merger ! (Step, (self, watcher), progress)

      this.stepMergeRef = Option(watcher)
      this.wip = Option(progress)
      this.workDone = 0
      mainLoop()
    } else {
      checkBeginMergeThenLoop()
    }
  }

  private def checkBeginMergeThenLoop(): Unit = {
    if(aReader.isDefined && bReader.isDefined && stepMergeRef.isEmpty) {
      val merger = beginMerge()
      this.stepMergeRef = Option(merger)
      this.workDone = 0
    }
    mainLoop()
  }

  private def beginMerge(): ActorRef = {
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
        case (Found, value) => sendReply(sender(), value)
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
      val (toFileName, pos) = (this.aReader, this.bReader) match {
        case (None, None) => (filename("A"), 1)
        case (_, None) => (filename("B"), 2)
        case (None, _) => (filename("C"), 3)
      }

      Utils.renameFile(fileName, toFileName)
      sendReply(sender(), true)

      val newReader = Option(RandomReader.open(toFileName))
      pos match {
        case 1 => this.aReader = newReader
        case 2 => {
          this.bReader = newReader
          checkBeginMergeThenLoop()
        }
        case 3 => this.cReader = newReader
      }
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
      val list = this.stepMergeRef match {
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
      val list = this.stepMergeRef match {
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
    var workLeftHere = 0
    if(this.bReader.isDefined && this.stepMergeRef.isDefined) {
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

    val workIncludingHere = previousWork + workToDoHere

    val delegateRef = this.next match {
      case None => None
      case Some(n) => Option(sendCall(n, context, (StepLevel, workIncludingHere, stepSize)))
    }

    val mergeRef = (workToDoHere > 0) match {
      case true => {
        this.stepMergeRef match {
          case Some(pid) => {
            val monitor = context.watch(pid)
            pid ! (Step, monitor, workToDoHere)
            Option(monitor)
          }
        }
      }
      case false => None
    }

    if(delegateRef.isEmpty && mergeRef.isEmpty) {
      this.stepCaller = stepFrom
      replyStepOk()
    } else {
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
          case Some(v) => v
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

case object StepLevel extends LevelOp
case object StepDone extends LevelOp
case object StepOk extends LevelOp

sealed abstract class LookupResponse
case object NotFound extends LookupResponse
case object Found extends LookupResponse
case object Delegate extends LookupResponse

sealed abstract class FoldWorkerOp
case object Initialize extends FoldWorkerOp