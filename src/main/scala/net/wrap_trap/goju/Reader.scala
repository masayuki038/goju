package net.wrap_trap.goju

import java.io._
import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import net.wrap_trap.goju.element.Element
import org.slf4j.LoggerFactory


/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Reader extends PlainRpc {
  val log = Logger(LoggerFactory.getLogger(Reader.getClass))

  def open(name: String): Index = {
    open(name, Random)
  }

  def open(name: String, config: FileConfig): Index = {
    config match {
      case Sequential => {
        SequentialReadIndex(
          buildInputStream(name),
          name,
          config
        )
      }
      case _ => {
        val file = new RandomAccessFile(name, "r")
        val fileInfo = new File(name)
        val buf = new Array[Byte](Constants.FILE_FORMAT.length)
        file.read(buf)
        val fileFormat = Utils.fromBytes(buf)
        if(Constants.FILE_FORMAT != fileFormat) {
          throw new IllegalStateException("Invalid file format: " + fileFormat)
        }
        file.seek(fileInfo.length - 8)
        val rootPos = file.readLong

        file.seek(fileInfo.length - 12)
        val bloomSize = file.readInt
        val bloomBuffer = new Array[Byte](bloomSize)
        file.seek(fileInfo.length - 12 - bloomSize)
        file.read(bloomBuffer)
        val bloom = SerDes.deserializeBloom(bloomBuffer)
        val node = readNode(file, rootPos)
        RandomReadIndex(
          file,
          name,
          config,
          node,
          Option(bloom)
        )
      }
    }
  }

  def readNode(file: RandomAccessFile, rootPos: Long): Option[ReaderNode] = {
    file.seek(rootPos)
    val len = file.readInt
    if(len != 0) {
      val level = file.readShort
      val buf = new Array[Byte](len - 2)
      file.read(buf)
      if(log.underlying.isDebugEnabled) {
        Utils.dumpBinary(buf, "readNode#buf")
      }
      val entryList = Utils.decodeIndexNodes(buf, Compress(Constants.COMPRESS_PLAIN))
      Option(ReaderNode(level, entryList))
    } else {
      None
    }
  }

  private def buildInputStream(name: String): DataInputStream = {
    val settings = Settings.getSettings
    val bufferPoolSize = settings.getInt("read_buffer_size", 524288)
    new DataInputStream(new BufferedInputStream(new FileInputStream(name), bufferPoolSize))
  }
}

class Reader extends PlainRpc with Actor {
  def receive = {
    case (PlainRpcProtocol.cast) => ???
  }
}

case class ReaderNode(level: Int, members: List[Element] = List.empty)

trait Index {
  val name: String
  val config: FileConfig
  val root: Option[ReaderNode]
  val bloom: Option[Bloom]
}

case class SequentialReadIndex(file: DataInputStream,
                 name: String,
                 config: FileConfig,
                 root: Option[ReaderNode] = None,
                 bloom: Option[Bloom] = None) extends Index

case class RandomReadIndex(file: RandomAccessFile,
                               name: String,
                               config: FileConfig,
                               root: Option[ReaderNode] = None,
                               bloom: Option[Bloom] = None) extends Index

sealed abstract class FileConfig
case object Sequential extends FileConfig
case object Folding extends FileConfig
case object Random extends FileConfig
case class Other(symbol: Symbol, value: Any) extends FileConfig