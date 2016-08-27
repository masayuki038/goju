package net.wrap_trap.goju

import java.io.{ByteArrayOutputStream, FileOutputStream, BufferedOutputStream, DataOutputStream}
import scala.concurrent.duration._

import akka.actor.{ActorRef, Props, ActorSystem, Actor}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import net.wrap_trap.goju.element.{KeyValue, KeyRef, Element}
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.Constants._


/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Writer extends PlainRpc {

  def open(name: String): ActorRef = {
    val system = ActorSystem("system")
    system.actorOf(Props(new Writer(name)))
  }

  def add(actorRef: ActorRef, element: Element) = {
    if(!element.expired()) {
      cast(actorRef, ('add, element))
    }
  }

  def count(actorRef: ActorRef): Int = {
    implicit val timeout = Timeout(5 seconds)
    call(actorRef, ('count)).asInstanceOf[Int]
  }

  def close(actorRef: ActorRef) = {
    implicit val timeout = Timeout(5 seconds)
    call(actorRef, ('close))
  }
}

class Writer(val name: String, var state: Option[State] = None) extends PlainRpc with Actor {
  val log = Logger(LoggerFactory.getLogger(Writer.getClass))

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
          appendNode(0, kv)
        }
      }
    }
  }

  def handleCall(msg: Any): Any = {
    msg match {
      case ('count) => {
        this.state match {
          case Some(s) => s.valueCount + s.tombstoneCount
        }
      }
      case ('close) => {
        archiveNodes
      }
    }
  }

  def doOpen() = {
    val settings = Settings.getSettings
    val writeBufferSize = settings.getInt("write_buffer_size", 512 * 1024)
    new DataOutputStream(
      new BufferedOutputStream(new FileOutputStream(this.name), writeBufferSize)
    )
  }

  def archiveNodes(): Unit = {
    this.state match {
      case Some(s) => s.nodes match {
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
            this.state = Option(
              s.copy(
                indexFile = None,
                indexFilePos = 0
              )
            )
          }
        }
        case List(node) if node.level > 0 && node.isInstanceOf[KeyRef] => {
          this.state = Option(s.copy(nodes = List.empty[WriterNode]))
          archiveNodes
        }
        case List(_, _*)  => {
          flushNodeBuffer
          archiveNodes
        }
      }
    }
  }

  def appendNode(level: Int, element: Element): Unit = {
    this.state match {
      case Some(s) => s.nodes match {
        case List() => {
          this.state = Option(s.copy(nodes = WriterNode(level) :: s.nodes))
          appendNode(level, element)
        }
        case List(node, _*) if(level < node.level) => {
          this.state = Option(s.copy(nodes = WriterNode(node.level) :: s.nodes.tail))
          appendNode(level, element)
        }
        case  List(node, _*) => {
          node.members match {
            case List() => {
              // do nothing
            }
            case List(member, _*) => {
              if(Utils.compareBytes(element.key(), member.key()) < 0) {
                throw new IllegalStateException("key < prevKey");
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
          val newState = s.copy(nodes = currentNode :: s.nodes.tail, valueCount = vc1, tombstoneCount = tc1)
          this.state = Option(newState)

          if(newSize >= newState.blockSize) {
            flushNodeBuffer
          }
        }
      }
    }
  }

  def flushNodeBuffer() = {
    this.state match {
      case Some(s) => s.nodes match {
        case  node::rest => {
          val level = node.level

          val orderedMembers = node.members.reverse
          val blockData = Utils.encodeIndexNodes(orderedMembers, Compress(Constants.COMPRESS_PLAIN))
          if(log.underlying.isDebugEnabled) {
            Utils.dumpBinary(blockData,"flushNode#blockData" )
          }
          val data = Utils.to4Bytes(blockData.size + 2) ++ Utils.to2Bytes(level) ++ blockData

          s.indexFile match {
            case Some(file) => file.write(data)
          }

          val posLen = new KeyRef(orderedMembers.head.key, s.indexFilePos, blockData.size + 6)
          appendNode(level + 1, posLen)
          val newState = s.copy(
            nodes = rest,
            indexFilePos = s.indexFilePos + data.size,
            lastNodePos = s.indexFilePos,
            lastNodeSize = data.size
          )
          this.state = Option(newState)
        }
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