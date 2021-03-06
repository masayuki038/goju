package net.wrap_trap.goju

import scala.annotation.tailrec
import java.io._

import scala.concurrent.duration._
import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.Props
import akka.util.Timeout
import net.wrap_trap.goju.element.Element
import net.wrap_trap.goju.element.KeyRef
import net.wrap_trap.goju.element.KeyValue
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.Constants._

import scala.language.postfixOps

/**
 * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
object Writer extends PlainRpcClient {

  def open(path: String): ActorRef = {
    val fileName = new File(path).getName
    Supervisor.createActor(
      Props(classOf[Writer], path, None),
      "writer-%s-%d".format(fileName, System.currentTimeMillis))
  }

  def open(path: String, context: ActorContext): ActorRef = {
    val fileName = new File(path).getName
    context.actorOf(
      Props(classOf[Writer], path, None),
      "writer-%s-%d".format(fileName, System.currentTimeMillis))
  }

  def add(actorRef: ActorRef, element: Element): Unit = {
    if (!element.expired()) {
      cast(actorRef, ('add, element))
    }
  }

  def count(actorRef: ActorRef): Int = {
    implicit val timeout: Timeout = Timeout(30 seconds)
    call(actorRef, 'count).asInstanceOf[Int]
  }

  def close(actorRef: ActorRef): Unit = {
    implicit val timeout: Timeout = Timeout(30 seconds)
    call(actorRef, 'close)
    Supervisor.stop(actorRef)
  }
}

class Writer(val name: String, var state: Option[State] = None) extends PlainRpc {
  val NODE_SIZE: Int = 8 * 1024

  override def preStart(): Unit = {
    super.preStart
    val settings = Settings.getSettings
    val fileFormat = Utils.toBytes(Constants.FILE_FORMAT)
    val fileWriter = doOpen()
    fileWriter.write(fileFormat)

    state = Option(
      State(
        Option(fileWriter),
        fileFormat.length,
        0L,
        0,
        List.empty[WriterNode],
        name,
        new Bloom(settings.getInt("size", 2048)),
        settings.getInt("block_size", NODE_SIZE),
        List.empty[Any]
      )
    )
  }

  override def postStop(): Unit = {
    close()
    super.postStop
  }

  def receive: Receive = {
    case (PlainRpcProtocol.cast, msg) => handleCast(msg)
    case (PlainRpcProtocol.call, msg) =>
      sendReply(sender, handleCall(msg))
  }

  def handleCast(msg: Any): Unit = {
    msg match {
      case ('add, kv: KeyValue) =>
        if (!kv.expired) {
          val newState = appendNode(0, kv, this.state.get)
          this.state = Option(newState)
        }
      case msg => throw new IllegalStateException("An unexpected message received. msg: " + msg)
    }
  }

  def handleCall(msg: Any): Any = {
    msg match {
      case ('add, kv: KeyValue) =>
        if (!kv.expired) {
          val newState = appendNode(0, kv, this.state.get)
          this.state = Option(newState)
        }
        true
      case 'count =>
        this.state match {
          case Some(s) => s.valueCount + s.tombstoneCount
          case None => throw new IllegalStateException("this.state is not defined")
        }
      case 'close =>
        close()
        true
      case msg => throw new IllegalStateException("An unexpected message received. msg: " + msg)
    }
  }

  def doOpen(): DataOutputStream = {
    val settings = Settings.getSettings
    val writeBufferSize = settings.getInt("write_buffer_size", 512 * 1024)
    new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(this.name), writeBufferSize)
    )
  }

  def archiveNodes(s: State): State = {
    log.debug("archiveNode, file: %s".format(s.name))
    s.nodes match {
      case List() =>
        val bloomBin = SerDes.serializeBloom(s.bloom)
        val rootPos = s.lastNodePos match {
          case 0L =>
            s.indexFile.foreach(file => {
              file.writeInt(0)
              file.writeShort(0)
            })
            FIRST_BLOCK_POS
          case _ => s.lastNodePos
        }
        using(new ByteArrayOutputStream) { baos =>
          baos.write(Utils.to4Bytes(0))
          baos.write(bloomBin)
          baos.write(Utils.to4Bytes(bloomBin.length))
          baos.write(Utils.to8Bytes(rootPos))
          s.indexFile.foreach(file => {
            file.write(baos.toByteArray)
            file.close()
          })
          s.copy(
            indexFile = None,
            indexFilePos = 0
          )
        }
      case List(node)
          if node.level > 0 && node.members.size == 1 && node.members.head.isInstanceOf[KeyRef] =>
        archiveNodes(s.copy(nodes = List.empty[WriterNode]))
      case List(_, _*) =>
        val newState = flushNodeBuffer(s)
        archiveNodes(newState)
    }
  }

  @tailrec
  final def appendNode(level: Int, element: Element, s: State): State = {
    log.debug("appendNode, level: %d, element: %s, file: %s".format(level, element, s.name))
    s.nodes match {
      case List() =>
        val newState = s.copy(nodes = WriterNode(level) :: s.nodes)
        appendNode(level, element, newState)
      case List(node, _*) if level < node.level =>
        val newState = s.copy(nodes = WriterNode(level = node.level - 1) :: s.nodes)
        appendNode(level, element, newState)
      case List(node, _*) =>
        node.members match {
          case List() =>
          // do nothing
          case List(member, _*) =>
            if (Utils.compareBytes(element.key(), member.key()) < 0) {
              Utils.dumpBinary(element.key().bytes, "element.key()")
              Utils.dumpBinary(member.key().bytes, "member.key()")
              throw new IllegalStateException(s"""element.key < member.key""")
            }
        }
        val newSize = node.size + element.estimateNodeSizeIncrement
        s.bloom.add(element.key())
        val (tc1, vc1) = node.level match {
          case 0 =>
            if (element.expired()) {
              (s.tombstoneCount + 1, s.valueCount)
            } else {
              (s.tombstoneCount, s.valueCount + 1)
            }
          case _ => (s.tombstoneCount, s.valueCount)
        }
        val currentNode = node.copy(members = element :: node.members, size = newSize)
        val newState = s.copy(
          nodes = currentNode :: s.nodes.tail,
          valueCount = vc1,
          tombstoneCount = tc1
        )

        if (newSize >= newState.blockSize) {
          flushNodeBuffer(newState)
        } else {
          newState
        }
    }
  }

  def flushNodeBuffer(s: State): State = {
    log.debug("flushNodeBuffer, file: %s".format(s.name))
    s.nodes match {
      case node :: rest =>
        val level = node.level

        val orderedMembers = node.members.reverse
        val blockData = Utils.encodeIndexNodes(orderedMembers, Compress(Constants.COMPRESS_PLAIN))
        val dumpBuffer =
          Settings.getSettings.getBoolean("goju.debug.dump_buffer", default = false)
        if (dumpBuffer) {
          Utils.dumpBinary(blockData, "flushNode#blockData")
        }
        val data = Utils.to4Bytes(blockData.length + 2) ++ Utils.to2Bytes(level) ++ blockData
        s.indexFile.foreach(_.write(data))

        val posLen = KeyRef(orderedMembers.head.key(), s.indexFilePos, blockData.length + 6)
        val newState = s.copy(
          nodes = rest,
          indexFilePos = s.indexFilePos + data.length,
          lastNodePos = s.indexFilePos,
          lastNodeSize = data.length
        )
        appendNode(level + 1, posLen, newState)
      case _ => throw new IllegalStateException("Unexpected s.nodes: %s".format(s.nodes))
    }
  }

  private def close(): Unit = {
    log.debug("close")
    val newState = archiveNodes(this.state.get)
    this.state = Option(newState)
  }
}

case class WriterNode(level: Int, members: List[Element] = List.empty, size: Int = 0)
case class State(
    indexFile: Option[DataOutputStream],
    indexFilePos: Int,
    lastNodePos: Long,
    lastNodeSize: Int,
    nodes: List[WriterNode],
    name: String,
    bloom: Bloom,
    blockSize: Int,
    opts: List[Any],
    valueCount: Int = 0,
    tombstoneCount: Int = 0)
