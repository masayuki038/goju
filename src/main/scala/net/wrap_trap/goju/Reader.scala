package net.wrap_trap.goju

import java.io._
import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.element.{Element, KeyValue, KeyRef}
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
          new File(name),
          config
        )
      }
      case _ => {
        val randomAccessFile = new RandomAccessFile(name, "r")
        val fileInfo = new File(name)
        val buf = new Array[Byte](Constants.FILE_FORMAT.length)
        randomAccessFile.read(buf)
        val fileFormat = Utils.fromBytes(buf)
        if(Constants.FILE_FORMAT != fileFormat) {
          throw new IllegalStateException("Invalid file format: " + fileFormat)
        }
        randomAccessFile.seek(fileInfo.length - 8)
        val rootPos = randomAccessFile.readLong

        randomAccessFile.seek(fileInfo.length - 12)
        val bloomSize = randomAccessFile.readInt
        val bloomBuffer = new Array[Byte](bloomSize)
        randomAccessFile.seek(fileInfo.length - 12 - bloomSize)
        randomAccessFile.read(bloomBuffer)
        val bloom = SerDes.deserializeBloom(bloomBuffer)
        val node = readNode(randomAccessFile, rootPos)
        RandomReadIndex(
          randomAccessFile,
          name,
          new File(name),
          config,
          node,
          Option(bloom)
        )
      }
    }
  }

  def readNode(file: RandomAccessFile, posLen: KeyRef): Option[ReaderNode] = {
    file.seek(posLen.pos + 4)
    val level = file.readShort
    val data = new Array[Byte](posLen.len - 4 - 2)
    if(file.read(data) != data.length) {
      Utils.dumpBinary(data, "data")
      throw new IllegalStateException("Failed to read data.")
    }
    val entryList = Utils.decodeIndexNodes(data, Compress(Constants.COMPRESS_PLAIN))
    Option(ReaderNode(level, entryList))
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

  def destroy(indexFile: Index) = {
    indexFile.close
    indexFile.delete
  }

  def serialize(sequentialReadIndex: SequentialReadIndex): (SequentialReadIndex, Long) = {
    (sequentialReadIndex, sequentialReadIndex.elementInputStream.pointer)
  }

  def deserialize(serialized: (SequentialReadIndex, Long)): Index = {
    val (index, pos) = serialized
    val newIndex = Reader.open(index.name, index.config)
    newIndex.skip(pos)
    newIndex
  }

  def fold(func: (List[Element], Element) => List[Element], acc0: List[Element], index: RandomReadIndex): List[Element] = {
    val node = readNode(index.randomAccessFile, Constants.FIRST_BLOCK_POS)
    fold(index.randomAccessFile, func, node, acc0)
  }

  def fold(file: RandomAccessFile, func: (List[Element], Element) => List[Element], node: Option[ReaderNode], acc0: List[Element]): List[Element] = {
    node match {
      case Some(n) => {
        n.level match {
          case 0 => {
            val acc1 = n.members.foldLeft(acc0) { (acc, element) => func(acc, element) }
            fold(file, func, acc1)
          }
          case _ => fold(file, func, acc0)
        }
      }
    }
  }

  def fold(file: RandomAccessFile, func: (List[Element], Element) => List[Element], acc0: List[Element]): List[Element] = {
    nextLeafNode(file) match {
      case None => acc0
      case node => fold(file, func, node, acc0)
    }
  }

  def lookupNode(file: RandomAccessFile, k: Key, node: ReaderNode, pos: Long): Long = {
    //    node.level match {
    //      case 0 => pos
    //      case _ => findStart
    //    }
    0L
  }

  def findStart(key: Key, members: List[Element]): Option[KeyRef] = {
    members match {
      case List(p: KeyRef, KeyRef(k2, _, _), _*) if key < k2 => Option(p)
      case List(posLen: KeyRef) => Option(posLen)
      case _ => find1(key, members)
    }
  }

  def find1(key: Key, members: List[Element]): Option[KeyRef] = {
    members match {
      case List(KeyRef(k1, pos, len), KeyValue(k2, _, _), _*) if key > k1 && key < k2 => Option(KeyRef(k1, pos, len))
      case List(KeyRef(k1, pos, len)) if key >= k1 => Option(KeyRef(k1, pos, len))
      case List(_, _) => None
      case List(_, _*) => find1(key, members.tail)
    }
  }

  def recursiveFind(file: RandomAccessFile, fromKey: Key, n: Int, childPos: KeyRef): Option[KeyRef] = {
    n match {
      case 1 => Option(childPos)
      case m if m > 1 => {
        readNode(file, childPos) match {
          case Some(childNode) => findLeafNode(file, fromKey, childNode, childPos)
          case None => None
        }
      }
    }
  }

  def findLeafNode(file: RandomAccessFile, fromKey: Key, node: ReaderNode, posLen: KeyRef): Option[KeyRef] = {
    if(node.level == 0) {
      return Option(posLen)
    }

    findStart(fromKey, node.members) match {
      case Some(childPosLen) => recursiveFind(file, fromKey, node.level, childPosLen)
      case None => None
    }
  }

  def nextLeafNode(file: RandomAccessFile): Option[ReaderNode] = {
    val bytes = new Array[Byte](6)
    val read = file.read(bytes, 0, 6)
    if(read == 6) {
      readHeader(bytes) match {
        case (len: Long, _) if len == 0 => None
        case (len: Long, level: Int) if level == 0 => {
          val buf = new Array[Byte]((len - 2).toInt) // @TODO Long to Int
          file.read(buf)
          val entryList = Utils.decodeIndexNodes(buf, Compress(Constants.COMPRESS_PLAIN))
          Option(ReaderNode(level, entryList))
        }
        case (len: Long, level: Int) => {
          file.seek((len - 2).toInt) // @TODO Long to Int
          nextLeafNode(file)
        }
      }
    } else {
      None
    }
  }

  def getValue(keyValue: KeyValue): Constants.Value = {
    keyValue.value
  }

  def rangeFoldFromHere(func: (Element, List[Element]) => List[Element],
                        acc0: List[Element],
                        file: RandomAccessFile,
                        range: KeyRange,
                        limit: Int): List[Element] = {
    nextLeafNode(file) match {
      case None => acc0
      case Some(node) => {
        foldUntilStop((keyValue, acc0, limit) => {
          (keyValue, acc0, limit) match {
            case (_, acc, 0) => (Stop, acc, 0)
            case (e, acc, _) if !range.keyInToRange(e.key) => (Stop, acc, 0)
            case (e, acc, _) if e.tombstoned && range.keyInFromRange(e.key) => {
              if(e.expired) {
                (Continue, acc, limit)
              } else {
                (Continue, func(e, acc), limit)
              }
            }
            case (e, acc, limit) if range.keyInFromRange(e.key) => {
              if(e.expired) {
                (Continue, acc, limit)
              } else {
                (Continue, func(e, acc), limit - 1)
              }
            }
          }
        }, acc0, limit, node.members) match {
          case (Stopped, result, _) => result
          case (Ok, acc1, limit) => rangeFoldFromHere(func, acc1, file, range, limit)
        }
      }
    }
  }

  def foldUntilStop(func: (Element, List[Element], Int) => (FoldStatus, List[Element], Int),
                    acc: List[Element],
                    limit: Int,
                    members: List[Element]): (FoldStatus, List[Element], Int) = {
    foldUntilStop2(func, (Continue, acc, limit), members)
  }

  def foldUntilStop2(func: (Element, List[Element], Int) => (FoldStatus, List[Element], Int),
                     accWithStatus: (FoldStatus, List[Element], Int),
                     members: List[Element]): (FoldStatus, List[Element], Int) = {
    accWithStatus match {
      case (Stop, result, limit) => (Stopped, result, limit)
      case (Continue, acc, limit) if members.length == 0 => (Ok, acc, limit)
      case (Continue, acc, limit) => foldUntilStop2(func, func(members.head, acc, limit), members.tail)
    }
  }

  def readHeader(bytes: Array[Byte]): (Long, Int) = {
    using(new ElementInputStream(new ByteArrayInputStream(bytes))) { eis =>
      (eis.readInt.toLong, eis.readShort.toInt)
    }
  }

  private def buildInputStream(name: String): ElementInputStream = {
    val settings = Settings.getSettings
    val bufferPoolSize = settings.getInt("read_buffer_size", 524288)
    new ElementInputStream(new BufferedInputStream(new FileInputStream(name), bufferPoolSize))
  }
}

class Reader extends PlainRpc with Actor {
  def receive = {
    case (PlainRpcProtocol.cast) => ???
  }
}

case class ReaderNode(level: Int, members: List[Element] = List.empty)

trait Index {
  val log = Logger(LoggerFactory.getLogger(this.getClass))

  val name: String
  val file: File
  val config: FileConfig
  val root: Option[ReaderNode]
  val bloom: Option[Bloom]

  def delete(): Unit = {
    if(!file.delete) {
      log.warn("Failed to delete file: " + name)
    }
  }

  def skip(n: Long): Unit
  def close(): Unit
}

case class SequentialReadIndex(elementInputStream: ElementInputStream,
                               name: String,
                               file: File,
                               config: FileConfig,
                               root: Option[ReaderNode] = None,
                               bloom: Option[Bloom] = None) extends Index {

  def skip(n: Long): Unit = {
    elementInputStream.skip(n)
  }

  def close(): Unit = {
    elementInputStream.close
  }
}

case class RandomReadIndex(randomAccessFile: RandomAccessFile,
                               name: String,
                               file: File,
                               config: FileConfig,
                               root: Option[ReaderNode] = None,
                               bloom: Option[Bloom] = None) extends Index {

  def skip(n: Long): Unit = {
    randomAccessFile.seek(n)
  }


  def close(): Unit = {
    randomAccessFile.close
  }
}

sealed abstract class FileConfig
case object Sequential extends FileConfig
case object Folding extends FileConfig
case object Random extends FileConfig
case class Other(symbol: Symbol, value: Any) extends FileConfig

sealed abstract class FoldResult
case object Done extends FoldResult

sealed abstract class FoldStatus
case object Stop extends FoldStatus
case object Stopped extends FoldStatus
case object Continue extends FoldStatus
case object Ok extends FoldStatus