package net.wrap_trap.goju

import java.io.{OutputStream, FileOutputStream, BufferedOutputStream}

import akka.actor.{Props, ActorSystem, Actor}
import net.wrap_trap.goju.samples.HelloAkka
import org.joda.time.DateTime

import scala.io.Source

import net.wrap_trap.goju.Constants._

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Writer {

  def open(name: String) = {
    val system = ActorSystem("system")
    system.actorOf(Props(new Writer(name)))
  }
}

class Writer(val name: String, var state: Option[State] = None) extends Actor {

  val NODE_SIZE = 8*1024

  override def preStart() = {
    super.preStart
    val settings = Settings.getSettings
    val fileFormat = Utils.toBytes(Constants.FILE_FORMAT)
    val fileWriter = doOpen
    fileWriter.write(fileFormat)

    state = Option(
      State(
        fileWriter,
        fileFormat.length,
        0L,
        0,
        List.empty[Node],
        name,
        new Bloom(settings.getInt("size", 2048)),
        settings.getInt("block_size", NODE_SIZE),
        List.empty[Any]
      )
    )
  }

  def receive = {
    case (PlainRpcProtocol.cast, msg) => handleCast(msg)
    case (PlainRpcProtocol.call, msg) => handleCall(msg)
  }

  def handleCast(msg: Any) = {
    msg match {
      case ('add, key, (Constants.TOMBSTONE, ts: DateTime)) => {
        if(!Utils.hasExpired(ts)) {
          // appendNode
        }
      }
    }
  }

  def handleCall(msg: Any) = {

  }


  def doOpen() = {
    val settings = Settings.getSettings
    val writeBufferSize = settings.getInt("write_buffer_size", 512 * 1024)
    new BufferedOutputStream(new FileOutputStream(this.name), writeBufferSize)
  }

  def appendNode(level: Int, key: Key, value: Value): Unit = {
    this.state match {
      case Some(s) => s.nodes match {
        case List() => {
          this.state = Option(s.copy(nodes = Node(level) :: s.nodes))
          appendNode(level, key, value)
        }
        case List(node, _*) if(level < node.level) => {
          this.state = Option(s.copy(nodes = Node(node.level) :: s.nodes.tail))
          appendNode(level, key, value)
        }
        case  List(node, _*) => {
          node.members match {
            case List((prevKey, _), _*) => {
              if(Utils.compareBytes(key, prevKey) < 0) {
                throw new IllegalStateException("key < prevKey");
              }
              val newSize = node.size + Utils.estimateNodeSizeIncrement(key, value)
              s.bloom.add(key)
              val (tc1, vc1) = node.level match {
                case 0 => value match {
                  case _: ExpValue => (s.tombstoneCount + 1, s.valueCount)
                  case _ => (s.tombstoneCount, s.valueCount + 1)
                }
                case _ => (s.tombstoneCount, s.valueCount)
              }
              val currentNode = node.copy(members = (key, value) :: node.members, size = newSize)
              val newState = s.copy(nodes = currentNode :: s.nodes.tail, valueCount = vc1, tombstoneCount = tc1)
              this.state = Option(newState)

              if(newSize >= newState.blockSize) {
                flushNodeBuffer
              }
            }
          }
        }
      }
    }
  }

  def flushNodeBuffer() = {
//    this.state match {
//      case Some(s) => s.nodes match {
//        case  List(node, _*) => {
//          val orderedMembers = node.members.reverse
//
//        }
//      }
//    }
  }
}

case class Node(level: Int, members: List[(Key, Value)] = List.empty, size: Int = 0)
case class State(indexFile: OutputStream,
                  indexFilePos: Int,
                  lastNodePos: Long,
                  lastNodeSize: Int,
                  nodes: List[Node],
                  name: String,
                  bloom: Bloom,
                  blockSize: Int,
                  opts: List[Any],
                  valueCount: Int = 0,
                  tombstoneCount: Int = 0)

