package net.wrap_trap.goju

import scala.annotation.tailrec

import java.io.{ByteArrayOutputStream, FileOutputStream, BufferedOutputStream, DataOutputStream}
import scala.concurrent.duration._

import akka.actor.{ActorRef, Props, Actor}
import akka.util.Timeout
import akka.event.Logging

import net.wrap_trap.goju.element.{KeyValue, KeyRef, Element}
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.Constants._


/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Writer extends PlainRpcClient {

  def open(name: String): ActorRef = {
    Utils.getActorSystem.actorOf(Props(new Writer(name)), "writer-" + System.currentTimeMillis)
  }

  def add(actorRef: ActorRef, element: Element) = {
    if(!element.expired()) {
      cast(actorRef, ('add, element))
    }
  }

  def count(actorRef: ActorRef): Int = {
    implicit val timeout = Timeout(30 seconds)
    call(actorRef, ('count)).asInstanceOf[Int]
  }

  def close(actorRef: ActorRef) = {
    implicit val timeout = Timeout(30 seconds)
    call(actorRef, ('close))
  }
}

class Writer(val name: String, var state: Option[State] = None) extends PlainRpc with Actor {
  val log = Logging(context.system, this)

  val NODE_SIZE = 8*1024

  override def preStart() = {
    super.preStart
    val settings = Settings.getSettings
    val fileFormat = Utils.toBytes(Constants.FILE_FORMAT)
    val fileWriter = doOpen
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

  def receive = {
    case (PlainRpcProtocol.cast, msg) => handleCast(msg)
    case (PlainRpcProtocol.call, msg) => {
      sendReply(sender, handleCall(msg))
    }
  }

  def handleCast(msg: Any) = {
    msg match {
      case ('add, kv: KeyValue) => {
        if(!kv.expired) {
          val newState = appendNode(0, kv, this.state.get)
          this.state = Option(newState)
        }
      }
      case msg => throw new IllegalStateException("An unexpected message received. msg: " + msg)
    }
  }

  def handleCall(msg: Any): Any = {
    msg match {
      case ('add, kv: KeyValue) => {
        if(!kv.expired) {
          val newState = appendNode(0, kv, this.state.get)
          this.state = Option(newState)
        }
      }
      case ('count) => {
        this.state match {
          case Some(s) => s.valueCount + s.tombstoneCount
        }
      }
      case ('close) => {
        val newState = archiveNodes(this.state.get)
        this.state = Option(newState)
      }
      case msg => throw new IllegalStateException("An unexpected message received. msg: " + msg)
    }
  }

  def doOpen() = {
    val settings = Settings.getSettings
    val writeBufferSize = settings.getInt("write_buffer_size", 512 * 1024)
    new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(this.name), writeBufferSize)
    )
  }

  def archiveNodes(s: State): State = {
    log.debug("archiveNode, file: %s".format(s.name))
    s.nodes match {
      case List() => {
        val bloomBin = SerDes.serializeBloom(s.bloom)
        val rootPos = s.lastNodePos match {
          case 0L => {
            s.indexFile match {
              case Some(file) => {
                file.writeInt(0)
                file.writeShort(0)
              }
            }
            FIRST_BLOCK_POS
          }
          case _ => s.lastNodePos
        }
        using(new ByteArrayOutputStream) { baos =>
          baos.write(Utils.to4Bytes(0))
          baos.write(bloomBin)
          baos.write(Utils.to4Bytes(bloomBin.length))
          baos.write(Utils.to8Bytes(rootPos))
          s.indexFile match {
            case Some(file) => {
              file.write(baos.toByteArray)
              file.close
            }
          }
          s.copy(
            indexFile = None,
            indexFilePos = 0
          )
        }
      }
      case List(node) if node.level > 0 && node.members.size == 1 && node.members.head.isInstanceOf[KeyRef] => {
        archiveNodes(s.copy(nodes = List.empty[WriterNode]))
      }
      case List(_, _*)  => {
        val newState = flushNodeBuffer(s)
        archiveNodes(newState)
      }
    }
  }

  @tailrec
  final def appendNode(level: Int, element: Element, s: State): State = {
    log.debug("appendNode, level: %d, element: %s, file: %s".format(level, element, s.name))
    s.nodes match {
      case List() => {
        val newState = s.copy(nodes = WriterNode(level) :: s.nodes)
        return appendNode(level, element, newState)
      }
      case List(node, _*) if(level < node.level) => {
        val newState = s.copy(nodes = WriterNode(level = node.level - 1) :: s.nodes)
        return appendNode(level, element, newState)
      }
      case List(node, _*) => {
        node.members match {
          case List() => {
            // do nothing
          }
          case List(member, _*) => {
            if(Utils.compareBytes(element.key(), member.key()) < 0) {
              Utils.dumpBinary(element.key.bytes, "element.key()")
              Utils.dumpBinary(member.key.bytes, "member.key()")
              throw new IllegalStateException(s"""element.key < member.key""");
            }
          }
        }
        val newSize = node.size + element.estimateNodeSizeIncrement
        s.bloom.add(element.key())
        val (tc1, vc1) = node.level match {
          case 0 => element.expired() match {
            case true => (s.tombstoneCount + 1, s.valueCount)
            case _ => (s.tombstoneCount, s.valueCount + 1)
          }
          case _ => (s.tombstoneCount, s.valueCount)
        }
        val currentNode = node.copy(members = element :: node.members, size = newSize)
        val newState = s.copy(
          nodes = currentNode :: s.nodes.tail,
          valueCount = vc1,
          tombstoneCount = tc1
        )

        if(newSize >= newState.blockSize) {
          flushNodeBuffer(newState)
        } else {
          newState
        }
      }
    }
  }

  def flushNodeBuffer(s: State): State = {
    log.debug("flushNodeBuffer, file: %s".format(s.name))
    s.nodes match {
      case  node::rest => {
        val level = node.level

        val orderedMembers = node.members.reverse
        val blockData = Utils.encodeIndexNodes(orderedMembers, Compress(Constants.COMPRESS_PLAIN))
        if(log.isDebugEnabled) {
          Utils.dumpBinary(blockData,"flushNode#blockData" )
        }
        val data = Utils.to4Bytes(blockData.size + 2) ++ Utils.to2Bytes(level) ++ blockData

        s.indexFile match {
          case Some(file) => file.write(data)
        }

        val posLen = new KeyRef(orderedMembers.head.key, s.indexFilePos, blockData.size + 6)
        val newState = s.copy(
          nodes = rest,
          indexFilePos = s.indexFilePos + data.size,
          lastNodePos = s.indexFilePos,
          lastNodeSize = data.size
        )
        appendNode(level + 1, posLen, newState)
      }
    }
  }
}

case class WriterNode(level: Int, members: List[Element] = List.empty, size: Int = 0)
case class State(indexFile: Option[DataOutputStream],
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