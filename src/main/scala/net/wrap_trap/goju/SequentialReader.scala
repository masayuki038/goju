package net.wrap_trap.goju

import java.io.{File, FileInputStream, BufferedInputStream}

import net.wrap_trap.goju.element.Element

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object SequentialReader {
  def open(name: String): SequentialReader = {
    new SequentialReader(name)
  }

  def serialize(sequentialReader: SequentialReader): (SequentialReader, Long) = {
    (sequentialReader, sequentialReader.inputStream.pointer)
  }

  def deserialize(serialized: (SequentialReader, Long)): SequentialReader = {
    val (reader, pos) = serialized
    val newReader = SequentialReader.open(reader.name)
    newReader.skip(pos)
    newReader
  }
}

class SequentialReader(val name: String) extends Reader {
  val inputStream = buildInputStream(name)
  val file = new File(name)

  def skip(n: Long): Unit = {
    inputStream.skip(n)
  }

  def close(): Unit = {
   inputStream.close
  }

  def delete(): Unit = {
    if(!file.delete) {
      log.warn("Failed to delete file: " + name)
    }
  }

  def firstNode(): Option[List[Element]] = {
    this.inputStream.reset()
    readNode(PosLen(Constants.FIRST_BLOCK_POS)) match {
      case Some(firstNode) => {
        firstNode.level match {
          case 0 => Option(firstNode.members)
        }
      }
      case _ => None
    }
  }

  def nextNode(): Option[List[Element]] = {
    nextLeafNode() match {
      case Some(node) => {
        node.level match {
          case 0 => Option(node.members)
        }
      }
      case _ => None
    }
  }

  private def readNode(posLen: PosLen): Option[ReaderNode] = {
    posLen match {
      case PosLen(pos, Some(len)) => {
        this.inputStream.skip(pos + 4)
        val level = this.inputStream.readShort
        val data = new Array[Byte](len - 4 - 2)
        if(this.inputStream.read(data) != data.length) {
          Utils.dumpBinary(data, "data")
          throw new IllegalStateException("Failed to read data.")
        }
        val entryList = Utils.decodeIndexNodes(data, Compress(Constants.COMPRESS_PLAIN))
        Option(ReaderNode(level, entryList))
      }
      case PosLen(rootPos, _) => {
        this.inputStream.skip(rootPos)
        val len = this.inputStream.readInt
        if(len != 0) {
          val level = this.inputStream.readShort
          val buf = new Array[Byte](len - 2)
          this.inputStream.read(buf)
          if(log.underlying.isDebugEnabled) {
            Utils.dumpBinary(buf, "readNode#buf")
          }
          val entryList = Utils.decodeIndexNodes(buf, Compress(Constants.COMPRESS_PLAIN))
          Option(ReaderNode(level, entryList))
        } else {
          None
        }
      }
    }
  }

  private def nextLeafNode(): Option[ReaderNode] = {
    val bytes = new Array[Byte](6)
    val read = this.inputStream.read(bytes, 0, 6)
    if(read == 6) {
      readHeader(bytes) match {
        case (len: Long, _) if len == 0 => None
        case (len: Long, level: Int) if level == 0 => {
          val buf = new Array[Byte]((len - 2).toInt) // @TODO Long to Int
          this.inputStream.read(buf)
          val entryList = Utils.decodeIndexNodes(buf, Compress(Constants.COMPRESS_PLAIN))
          Option(ReaderNode(level, entryList))
        }
        case (len: Long, level: Int) => {
          this.inputStream.skip((len - 2).toInt) // @TODO Long to Int
          nextLeafNode
        }
      }
    } else {
      None
    }
  }

  private def buildInputStream(name: String): ElementInputStream = {
    val settings = Settings.getSettings
    val bufferPoolSize = settings.getInt("read_buffer_size", 524288)
    new ElementInputStream(new BufferedInputStream(new FileInputStream(name), bufferPoolSize))
  }
}
