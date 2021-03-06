package net.wrap_trap.goju

import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

import akka.actor._
import akka.util.Timeout
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.Element
import net.wrap_trap.goju.element.KeyValue
import org.hashids.Hashids
import org.hashids.syntax._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
object Level extends PlainRpcClient {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val callTimeout = Settings.getSettings.getInt("goju.level.call_timeout", 300)
  private implicit val timeout: Timeout = Timeout(callTimeout seconds)

  def open(dirPath: String, level: Int, owner: Option[ActorRef]): ActorRef = {
    Utils.ensureExpiry()
    Supervisor.createActor(
      Props(classOf[Level], dirPath, level, owner),
      "level%d-%d-%d".format(level, owner.hashCode, System.currentTimeMillis))
  }

  def open(
      dirPath: String,
      level: Int,
      owner: Option[ActorRef],
      context: ActorContext): ActorRef = {
    Utils.ensureExpiry()
    context.actorOf(
      Props(classOf[Level], dirPath, level, owner),
      "level%d-%d-%d".format(level, owner.hashCode, System.currentTimeMillis))
  }

  def level(ref: ActorRef): Int = {
    call(ref, Query) match {
      case ret: Int => ret
    }
  }

  def lookup(ref: ActorRef, key: Array[Byte]): Option[KeyValue] = {
    call(ref, Lookup(key, None)) match {
      case NotFound => None
      case kv: KeyValue => Option(kv)
    }
  }

  def lookup(ref: ActorRef, key: Array[Byte], f: Option[Value] => Unit): Unit = {
    cast(ref, LookupAsync(key, f))
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
    call(ref, UnmergedCount) match {
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
      case ignore: Exception =>
        log.warn("Failed to close ref: " + ref, ignore)
    }
  }

  def destroy(ref: ActorRef): Unit = {
    try {
      call(ref, Destroy)
    } catch {
      case ignore: Exception =>
        log.warn("Failed to close ref: " + ref, ignore)
    }
  }

  def snapshotRange(ref: ActorRef, foldWorkerRef: ActorRef, keyRange: KeyRange): Unit = {
    log.debug(
      "snapshotRange, ref: %s, foldWorkerRef: %s, keyRange: %s"
        .format(ref, foldWorkerRef, keyRange))
    val folders =
      call(ref, InitSnapshotRangeFold(None, foldWorkerRef, keyRange, List.empty[String]))
        .asInstanceOf[List[String]]
    foldWorkerRef ! Initialize(folders)
  }

  def blockingRange(ref: ActorRef, foldWorkerRef: ActorRef, keyRange: KeyRange): Unit = {
    log.debug(
      "blockingRange, ref: %s, foldWorkerRef: %s, keyRange.fromKey: %s, keyRange.toKey: %s"
        .format(ref, foldWorkerRef, keyRange.fromKey, keyRange.toKey))
    val folders =
      call(ref, InitBlockingRangeFold(None, foldWorkerRef, keyRange, List.empty[String]))
        .asInstanceOf[List[String]]
    foldWorkerRef ! Initialize(folders)
  }
}

class Level(val dirPath: String, val level: Int, val owner: Option[ActorRef])
    extends PlainRpc with Stash {
  private implicit val hashids: Hashids = Hashids.reference(this.hashCode.toString)

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
    Utils.ensureExpiry()
    val aFileName = filename("A")
    val bFileName = filename("B")
    val cFileName = filename("C")
    val mFileName = filename("M")

    Utils.deleteFile(filename("X"))
    Utils.deleteFile(filename("AF"))
    Utils.deleteFile(filename("BF"))
    Utils.deleteFile(filename("CF"))

    if (new File(mFileName).exists) {
      Utils.deleteFile(aFileName)
      Utils.deleteFile(bFileName)
      Utils.renameFile(mFileName, aFileName)

      this.aReader = Option(RandomReader.open(aFileName))

      if (new File(cFileName).exists) {
        Utils.renameFile(cFileName, bFileName)
        this.bReader = Option(RandomReader.open(bFileName))
        checkBeginMergeThenLoop0()
      } else {
        mainLoop()
      }
    } else {
      if (new File(bFileName).exists) {
        this.aReader = Option(RandomReader.open(aFileName))
        this.bReader = Option(RandomReader.open(bFileName))
        if (new File(cFileName).exists) {
          this.cReader = Option(RandomReader.open(cFileName))
        }
      } else {
        if (new File(cFileName).exists) {
          throw new IllegalStateException("Conflict: bFileName doesn't exist but cFileName exists")
        } else {
          if (new File(aFileName).exists) {
            this.aReader = Option(RandomReader.open(aFileName))
          }
          mainLoop()
        }
      }
    }
  }

  private def mainLoop(): Unit = {
    // loop
  }

  private def checkBeginMergeThenLoop0(): Unit = {
    log.debug("checkBeginMergeThenLoop0")
    if (aReader.isDefined && bReader.isDefined && mergePid.isEmpty) {
      val mergeRef = beginMerge()

      val progress = this.cReader match {
        case Some(_) => 2 * Utils.btreeSize(this.level)
        case _ => Utils.btreeSize(this.level)
      }
      val ref = System.nanoTime.hashid
      mergeRef ! (Step, ref, progress)

      this.mergePid = Option(mergeRef)
      this.stepMergeRef = Option(mergeRef)
      this.wip = Option(progress)
      this.workDone = 0
      mainLoop()
    } else {
      checkBeginMergeThenLoop()
    }
  }

  private def checkBeginMergeThenLoop(): Unit = {
    log.debug("checkBeginMergeThenLoop, %s, %s, %s".format(aReader, bReader, mergePid))
    if (aReader.isDefined && bReader.isDefined && mergePid.isEmpty) {
      log.debug(
        "checkBeginMergeThenLoop, aReader.isDefined && bReader.isDefined && mergePid.isEmpty")
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

    val merger = this.context.actorOf(
      Props(
        classOf[Merge],
        self,
        aFileName,
        bFileName,
        xFileName,
        Utils.btreeSize(this.level + 1),
        next.isEmpty),
      "merge-level%d-%d".format(this.level, System.currentTimeMillis)
    )
    context.watch(merger)
  }

  private def filename(prefix: String): String = {
    "%s/%s-%d.data".format(dirPath, prefix, this.level)
  }

  def receive: Actor.Receive = {
    case (PlainRpcProtocol.call, Lookup(key, from)) =>
      log.debug("receive: Lookup(call) key: %s, level: %s".format(key, level))
      val pid = from.getOrElse(sender)
      doLookup(key, List(this.cReader, this.bReader, this.aReader), this.next) match {
        case NotFound => sendReply(pid, NotFound)
        case (Found, kv) => sendReply(pid, kv)
        case (Delegate, next: ActorRef) => next ! (PlainRpcProtocol.call, Lookup(key, Option(pid)))
      }
    case (PlainRpcProtocol.cast, LookupAsync(key, f)) =>
      log.debug("receive: Lookup(cast) key: %s, level: %s".format(key, level))
      doLookup(key, List(this.cReader, this.bReader, this.aReader), this.next) match {
        case NotFound => f(None)
        case (Found, value: Option[Value]) => f(value)
        case (Delegate, pid: ActorRef) => pid ! (PlainRpcProtocol.call, LookupAsync(key, f))
      }
    case (PlainRpcProtocol.call, (Inject, fileName: String)) if cReader.isEmpty =>
      val from = sender
      log.debug("receive Inject && cReader.isEmpty: fileName: %s, from: %s".format(fileName, from))
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
        case 2 =>
          this.bReader = newReader
          checkBeginMergeThenLoop()
        case 3 => this.cReader = newReader
      }
      sendReply(from, true)
    case (PlainRpcProtocol.call, (Inject, fileName: String)) =>
      log.debug("receive Inject: fileName: %s".format(fileName))
      stash()
    case (PlainRpcProtocol.call, UnmergedCount) => sendReply(sender(), totalUnmerged())
    case (PlainRpcProtocol.cast, (SetMaxLevel, max: Int)) =>
      if (next.isDefined) {
        Level.setMaxLevel(next.get, max)
      }
      this.maxLevel = max
    case (PlainRpcProtocol.call, (BeginIncrementalMerge, stepSize: Int))
        if this.stepMergeRef.isEmpty && this.stepNextRef.isEmpty =>
      log.debug(
        "receive BeginIncrementalMerge (stepMergeRef.isEmpty && stepNextRef.isEmpty): stepSize: %d"
          .format(stepSize))
      sendReply(sender(), true)
      doStep(None, 0, stepSize)
    case (PlainRpcProtocol.call, (BeginIncrementalMerge, stepSize: Int)) =>
      log.debug("receive BeginIncrementalMerge: stepSize: %d".format(stepSize))
      log.warning(
        "this.stepMergeRef: %s, this.stepNextRef: %s".format(this.stepMergeRef, this.stepNextRef))
      stash()
    case (PlainRpcProtocol.call, AwaitIncrementalMerge)
        if this.stepMergeRef.isEmpty && this.stepNextRef.isEmpty =>
      sendReply(sender(), true)
    case (PlainRpcProtocol.call, (StepLevel, doneWork: Int, stepSize: Int))
        if this.stepMergeRef.isEmpty && this.stepCaller.isEmpty && this.stepNextRef.isEmpty =>
      doStep(Option(sender()), doneWork, stepSize)
    case (PlainRpcProtocol.call, Query) =>
      sendReply(sender(), this.level)
    case (merger: ActorRef, StepDone)
        if this.stepMergeRef.isDefined && merger == this.stepMergeRef.get =>
      log.debug("receive StepDone: ref: %s".format(merger))
      context.unwatch(merger)
      this.wip.foreach(w => {
        this.workDone += w
        this.wip = Option(0)
      })
      this.stepNextRef match {
        case Some(_) => // do nothing
        case None => replyStepOk()
      }
      this.stepMergeRef = None
      unstashAll()
    case (PlainRpcProtocol.reply, StepOk)
        if (this.stepNextRef.isDefined && sender == this.stepNextRef.get) && this.stepMergeRef.isEmpty =>
      log.debug(
        "receive: StepOk ((stepNextRef.isDefined && sender == this.stepNextRef.get) && this.stepMergeRef.isEmpty")
      context.unwatch(sender)
      this.stepNextRef = None
    case (PlainRpcProtocol.reply, StepOk) =>
      log.debug("receive: StepOk")
      log.warning(
        "this.level: %s, sender: %s, this.stepMergeRef: %s, this.stepNextRef: %s"
          .format(this.level, sender, this.stepMergeRef, this.stepNextRef))
      stash()
    case (PlainRpcProtocol.call, Close) =>
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
    case (PlainRpcProtocol.call, Destroy) =>
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
    case (PlainRpcProtocol.call, InitSnapshotRangeFold(gojuActor, workerPid, range, refList))
        if this.folding.isEmpty =>
      log.debug(
        "receive InitSnapshotRangeFold, workerPid: %s, range: %s, list: %s"
          .format(workerPid, range, refList))
      val (nextList, foldingPids) = (this.aReader, this.bReader, this.cReader) match {
        case (None, None, None) =>
          log.debug("InitSnapshotRangeFold, case (None, None None)")
          (refList, List.empty[ActorRef])
        case (_, None, None) =>
          log.debug("InitSnapshotRangeFold, case (_, None None)")
          log.debug("createLink from A to AF")
          Files.createLink(Paths.get(filename("AF")), Paths.get(filename("A")))
          val pid0 = startRangeFold(filename("AF"), workerPid, range)
          (pid0.toString :: refList, List(pid0))
        case (_, _, None) =>
          log.debug("InitSnapshotRangeFold, case (_, _, None)")
          log.debug("createLink from A to AF")
          Files.createLink(Paths.get(filename("AF")), Paths.get(filename("A")))
          val pidA = startRangeFold(filename("AF"), workerPid, range)
          log.debug("createLink from B to BF")
          Files.createLink(Paths.get(filename("BF")), Paths.get(filename("B")))
          val pidB = startRangeFold(filename("BF"), workerPid, range)
          (List(pidA.toString, pidB.toString) ::: refList, List(pidB, pidA))
        case (_, _, _) =>
          log.debug("InitSnapshotRangeFold, case (_, _, _)")
          log.debug("createLink from A to AF")
          Files.createLink(Paths.get(filename("AF")), Paths.get(filename("A")))
          val pidA = startRangeFold(filename("AF"), workerPid, range)
          log.debug("createLink from B to BF")
          Files.createLink(Paths.get(filename("BF")), Paths.get(filename("B")))
          val pidB = startRangeFold(filename("BF"), workerPid, range)
          log.debug("createLink from C to CF")
          Files.createLink(Paths.get(filename("CF")), Paths.get(filename("C")))
          val pidC = startRangeFold(filename("CF"), workerPid, range)
          (List(pidA.toString, pidB.toString, pidC.toString) ::: refList, List(pidC, pidB, pidA))
      }

      log.debug("InitSnapshotRangeFold, this.next: %s".format(this.next))
      val from = gojuActor match {
        case None => sender
        case Some(g) => g
      }
      this.next match {
        case Some(n) =>
          n ! (PlainRpcProtocol.call, InitSnapshotRangeFold(
            Option(from),
            workerPid,
            range,
            nextList))
        case _ => sendReply(from, nextList.reverse)
      }
      this.folding = foldingPids
    case (RangeFoldDone, pid: ActorRef, filePath: String) =>
      Utils.deleteFile(filePath)
      this.folding = this.folding.filter(p => p != pid)
    case (PlainRpcProtocol.call, InitBlockingRangeFold(gojuActor, workerPid, range, refList)) =>
      log.debug(
        "receive InitBlockingRangeFold, workerPid: %s, range: %s, list: %s"
          .format(workerPid, range, refList))
      val newRefList = (this.aReader, this.bReader, this.cReader) match {
        case (None, None, None) => refList
        case (Some(a), None, None) =>
          val aRef = System.nanoTime.hashid
          doRangeFold(a, workerPid, aRef, range)
          aRef :: refList
        case (Some(a), Some(b), None) =>
          val aRef = System.nanoTime.hashid
          doRangeFold(a, workerPid, aRef, range)

          val bRef = System.nanoTime.hashid
          doRangeFold(b, workerPid, bRef, range)

          List(aRef, bRef) ::: refList
        case (Some(a), Some(b), Some(c)) =>
          val aRef = System.nanoTime.hashid
          doRangeFold(a, workerPid, aRef, range)

          val bRef = System.nanoTime.hashid
          doRangeFold(b, workerPid, bRef, range)

          val cRef = System.nanoTime.hashid
          doRangeFold(b, workerPid, cRef, range)

          List(aRef, bRef, cRef) ::: refList
        case _ =>
          throw new IllegalStateException(
            "Unexpected file status, aReader: %s, bReader: %s, cReader: %s"
              .format(this.aReader, this.bReader, this.cReader))
      }
      val from = gojuActor match {
        case None => sender
        case Some(g) => g
      }
      this.next match {
        case Some(n) =>
          n ! (PlainRpcProtocol.call, InitBlockingRangeFold(
            Option(from),
            workerPid,
            range,
            newRefList))
        case _ => sendReply(from, newRefList.reverse)
      }
    case (PlainRpcProtocol.cast, (MergeDone, 0, outFileName: String)) =>
      log.debug("receive (MergeDone, count: 0, outFileName: %s)".format(outFileName))

      Utils.deleteFile(outFileName)
      closeAndDeleteAAndB()

      this.mergePid = None
      this.cReader match {
        case None => // do nothing
        case Some(reader) =>
          reader.close()
          val aFileName = filename("A")
          Utils.renameFile(filename("C"), aFileName)
          this.aReader = Option(RandomReader.open(aFileName))
          this.cReader = None
          unstashAll()
      }
    case (PlainRpcProtocol.cast, (MergeDone, count: Int, outFileName: String))
        if Utils.btreeSize(this.level) >= count && this.cReader.isEmpty && this.next.isEmpty =>
      log.debug("receive (MergeDone, count: %d, outFileName: %s)".format(count, outFileName))
      log.debug(
        "if Utils.btreeSize(this.level) >= count && this.cReader.isEmpty && this.next.isEmpty")

      val mFileName = filename("M")
      Utils.renameFile(outFileName, mFileName)
      closeAndDeleteAAndB()

      val aFileName = filename("A")
      Utils.renameFile(mFileName, aFileName)
      this.aReader = Option(RandomReader.open(aFileName))

      this.mergePid = None
      this.cReader match {
        case None => this.bReader = None
        case Some(reader) =>
          log.debug(
            "receive (MergeDone, count: %d, outFileName: %s), cReader.isDefined"
              .format(count, outFileName))
          reader.close()
          val bFileName = filename("B")
          Utils.renameFile(filename("C"), bFileName)
          this.bReader = Option(RandomReader.open(bFileName))
          this.cReader = None
          checkBeginMergeThenLoop()
          unstashAll()
      }
    case (PlainRpcProtocol.cast, (MergeDone, count: Int, outFileName: String)) =>
      log.debug("receive (MergeDone, count: %d, outFileName: %s)".format(count, outFileName))
      if (next.isEmpty) {
        val level = Level.open(this.dirPath, this.level + 1, this.owner, this.context)
        this.owner.foreach { o =>
          o ! (BottomLevel, this.level + 1)
        }
        this.next = Option(level)
        this.maxLevel = this.level + 1
      }
      this.next.foreach(n => {
        val mRef = sendCall(n, this.context, (Inject, outFileName))
        this.injectDoneRef = Option(mRef)
        this.mergePid = None
      })
    case (PlainRpcProtocol.reply, _: Boolean)
        if this.injectDoneRef.isDefined && sender == this.injectDoneRef.get =>
      log.debug(
        "received reply, true if this.injectDoneRef.isDefined && sender == this.injectDoneRef")
      this.context.unwatch(sender)
      closeAndDeleteAAndB()
      this.cReader match {
        case None => // do nothing
        case Some(reader) =>
          // If cReader already has been read halfway, lost the position of the reading by RandomReader#close
          reader.close()
          val aFileName = filename("A")
          Utils.renameFile(filename("C"), aFileName)
          this.aReader = Option(RandomReader.open(aFileName))
          this.bReader = None
          this.cReader = None
          unstashAll()
      }
    case Terminated(mRef) if this.stepMergeRef.isDefined && mRef == this.stepMergeRef.get =>
      log.debug(
        "received Terminated, e: %s, this.stepMergeRef.get: %s".format(mRef, this.stepMergeRef))
      this.stepNextRef match {
        case Some(_) => // do nothing
        case None => replyStepOk()
      }
      this.stepMergeRef = None
      this.wip = Option(0)
    case Terminated(mRef) if this.injectDoneRef.isDefined && mRef == this.injectDoneRef.get =>
      throw new IllegalStateException("injectDoneRef has down")
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

  private def doRangeFold(
      reader: RandomReader,
      workerPid: ActorRef,
      ref: String,
      range: KeyRange): Unit = {
    val (_, values) = reader.rangeFold((e, acc0) => {
      workerPid ! (LevelResult, ref, e)
      acc0
    }, (100, List.empty[Element]), range)

    values.length match {
      case range.limit => workerPid ! (LevelLimit, ref)
      case _ => workerPid ! (LevelDone, ref)
    }
  }

  private def startRangeFold(path: String, workerPid: ActorRef, range: KeyRange): ActorRef = {
    context.actorOf(
      Props(classOf[RangeFolder], path, workerPid, self, range),
      "rangeFolder-level%d-%s".format(this.level, path.replace('/', '-')))
  }

  private def destroyIfDefined(reader: Option[RandomReader]): Unit = {
    reader match {
      case Some(r) => r.destroy()
      case _ => // do nothing
    }
  }

  private def closeIfDefined(reader: Option[RandomReader]): Unit = {
    log.debug("closeIfDefined: reader: %s".format(reader))
    reader match {
      case Some(r) => r.close()
      case _ => // do nothing
    }
  }

  private def doStep(stepFrom: Option[ActorRef], previousWork: Int, stepSize: Int): Unit = {
    log.debug(
      "doStep: stepFrom: %s, previousWork: %d, stepSize: %d"
        .format(stepFrom, previousWork, stepSize))
    var workLeftHere = 0
    if (this.bReader.isDefined && this.mergePid.isDefined) {
      workLeftHere = Math.max(0, (2 * Utils.btreeSize(this.level)) - this.workDone)
    }
    val workUnit = stepSize
    val maxLevel = Math.max(this.maxLevel, this.level)
    val depth = maxLevel - Constants.TOP_LEVEL + 1
    val totalWork = depth * workDone
    val workUnitsLeft = Math.max(0, totalWork - previousWork)

    val workToDoHere = Settings.getSettings.getInt("merge.strategy", 1) match {
      case Constants.MERGE_STRATEGY_FAST => Math.min(workLeftHere, workUnit)
      case Constants.MERGE_STRATEGY_PREDICTABLE =>
        if (workLeftHere < depth * workUnit) {
          Math.min(workLeftHere, workUnit)
        } else {
          Math.min(workLeftHere, workUnitsLeft)
        }
    }

    log.debug(
      "doStep: workUnit: %d, maxLevel: %d, depth: %d, totalWork: %d, workUnitsLeft: %d, workToDoHere: %d"
        .format(workUnit, maxLevel, depth, totalWork, workUnitsLeft, workToDoHere))

    val workIncludingHere = previousWork + workToDoHere

    log.debug("doStep: workIncludingHere: %d".format(workIncludingHere))

    val delegateRef = this.next match {
      case None => None
      case Some(n) => Option(sendCall(n, context, (StepLevel, workIncludingHere, stepSize)))
    }

    log.debug("doStep: delegateRef: %s".format(delegateRef))

    val mergeRef = if (workToDoHere > 0) {
      this.mergePid.map(pid => {
        context.watch(pid)
        pid ! (Step, pid, workToDoHere)
        pid
      })
    } else {
      None
    }

    log.debug("doStep: mergeRef: %s".format(mergeRef))

    if (delegateRef.isEmpty && mergeRef.isEmpty) {
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

  @scala.annotation.tailrec
  private def doLookup(
      key: Array[Byte],
      list: List[Option[RandomReader]],
      next: Option[ActorRef]): Any = {
    list match {
      case List() =>
        next match {
          case Some(pid) => (Delegate, pid)
          case _ => NotFound
        }
      case List(None, _*) => doLookup(key, list.tail, next)
      case List(Some(reader), _*) =>
        reader.lookup(key) match {
          case Some(kv) => (Found, kv)
          case None =>
            // TODO if value is tombstoned, stopping to call doLoockup recursively and return None
            doLookup(key, list.tail, next)
        }
    }
  }
}
