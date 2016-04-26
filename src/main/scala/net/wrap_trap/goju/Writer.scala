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
        Array(),
        name,
        new Bloom(settings.getInt("size", 2048)),
        settings.getInt("block_size", NODE_SIZE),
        Array()
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
}

case class Node(level: Int, members: Array[(Key, Value)], size: Int = 0)
case class State(indexFile: OutputStream,
                  indexFilePos: Int,
                  lastNodePos: Long,
                  lastNodeSize: Int,
                  nodes: Array[Node],
                  name: String,
                  bloom: Bloom,
                  blockSize: Int,
                  opts: Array[Any],
                  valueCount: Int = 0,
                  tombstoneCount: Int = 0)

