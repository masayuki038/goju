package net.wrap_trap.goju

import java.io.{File, RandomAccessFile}

import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.element.{KeyRef, KeyValue, Element}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object RandomReader {
  def open(name: String): RandomReader = {
    new RandomReader(name)
  }
}

class RandomReader(val name: String) extends Reader {

  val randomAccessFile = new RandomAccessFile(name, "r")
  val file = new File(name)
  val buf = new Array[Byte](Constants.FILE_FORMAT.length)
  randomAccessFile.read(buf)
  val fileFormat = Utils.fromBytes(buf)
  if(Constants.FILE_FORMAT != fileFormat) {
    throw new IllegalStateException("Invalid file format: " + fileFormat)
  }

  randomAccessFile.seek(file.length - 8)
  val rootPos = randomAccessFile.readLong

  randomAccessFile.seek(file.length - 12)
  val bloomSize = randomAccessFile.readInt
  val bloomBuffer = new Array[Byte](bloomSize)
  randomAccessFile.seek(file.length - 12 - bloomSize)
  randomAccessFile.read(bloomBuffer)
  val bloom = SerDes.deserializeBloom(bloomBuffer)
  val root = readNode(PosLen(rootPos))

  log.debug("created, name: %s".format(this.name))

  def fold(func: (List[Element], Element) => List[Element], acc0: List[Element]): List[Element] = {
    val node = readNode(PosLen(Constants.FIRST_BLOCK_POS))
    foldInNode(func, node, acc0)
  }

  def foldInNode(func: (List[Element], Element) => List[Element], node: Option[ReaderNode], acc0: List[Element]): List[Element] = {
    node match {
      case Some(n) => {
        n.level match {
          case 0 => {
            val acc1 = n.members.foldLeft(acc0) { (acc, element) => func(acc, element) }
            fold(func, acc1)
          }
          case _ => foldInNode(func, acc0)
        }
      }
    }
  }

  def foldInNode(func: (List[Element], Element) => List[Element], acc0: List[Element]): List[Element] = {
    nextLeafNode() match {
      case None => acc0
      case node => foldInNode(func, node, acc0)
    }
  }

  def lookup(bytes: Array[Byte]): Option[KeyValue] = {
    val strKey = Utils.toHexStrings(bytes)
    log.debug("lookup, bytes: %s".format(strKey))
    val key = Key(bytes)
    this.bloom.member(key) match {
      case true => {
        log.debug("lookup, bytes: %s, bloom.member: true".format(strKey))
        lookupInNode(key) match {
          case Some(e: KeyValue) => {
            log.debug("lookup, bytes: %s, lookupInNode(key): true".format(strKey))
            if(e.tombstoned || e.expired) {
              None
            } else {
              Option(e)
            }
          }
          case Some(e: KeyRef) => {
            log.debug("lookup, bytes: %s, lookupInNode(key): Some(PosLen)".format(strKey))
            None
          }
          case None => {
            log.debug("lookup, bytes: %s, lookupInNode(key): false".format(strKey))
            None
          }
        }
      }
      case _ => {
        log.debug("lookup, bytes: %s, bloom.member: false".format(strKey))
        None
      }
    }
  }

  def rangeFold(func: (Element, (Int, List[Value])) => (Int, List[Value]),
                acc0: (Int, List[Value]),
                range: KeyRange
               ): (Int, List[Value]) = {
    range.fromKey <= firstKey(this.root.get) match {
      case true => {
        this.randomAccessFile.seek(Constants.FIRST_BLOCK_POS)
        rangeFoldFromHere(func, acc0, range, range.limit)
      }
      case false => {
        findLeafNode(range.fromKey, this.root.get, PosLen(Constants.FIRST_BLOCK_POS)) match {
          case Some(posLen) => {
            this.randomAccessFile.seek(posLen.pos)
            rangeFoldFromHere(func, acc0, range, range.limit)
          }
          case _ => {
            acc0
          }
        }
      }
    }
  }

  def rangeFoldFromHere(func: (Element, (Int, List[Value])) => (Int, List[Value]),
                        acc0: (Int, List[Value]),
                        range: KeyRange,
                        limit: Int): (Int, List[Value]) = {
    nextLeafNode() match {
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
            case (e, acc, limit) => {
              (Continue, acc, limit)
            }
          }
        }, acc0, limit, node.members) match {
          case (Stopped, result, _) => result
          case (Ok, acc1, limit) => rangeFoldFromHere(func, acc1, range, limit)
        }
      }
    }
  }

  private def readNode(posLen: PosLen): Option[ReaderNode] = {
    val dumpBuffer = Settings.getSettings().getBoolean("goju.debug.dump_buffer", false)
    posLen match {
      case PosLen(pos, Some(len)) => {
        this.randomAccessFile.seek(pos + 4)
        val level = this.randomAccessFile.readShort
        val data = new Array[Byte](len - 4 - 2)
        if(this.randomAccessFile.read(data) != data.length) {
          if(dumpBuffer) {
            Utils.dumpBinary(data, "data")
          }
          throw new IllegalStateException("Failed to read data.")
        }
        val entryList = Utils.decodeIndexNodes(data, Compress(Constants.COMPRESS_PLAIN))
        Option(ReaderNode(level, entryList))
      }
      case PosLen(rootPos, _) => {
        this.randomAccessFile.seek(rootPos)
        val len = this.randomAccessFile.readInt
        if(len != 0) {
          val level = this.randomAccessFile.readShort
          val buf = new Array[Byte](len - 2)
          this.randomAccessFile.read(buf)
          if(dumpBuffer) {
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

  private def lookupInNode(key: Key): Option[Element] = {
    val node = this.root.get
    node.level match {
      case 0 => findInLeaf(key, node.members)
      case _ => {
        find1(key, node.members) match {
          case Some(posLen) => {
            readNode(posLen) match {
              case Some(node) => lookupInNode2(node, key)
            }
          }
          case None => None
        }
      }
    }
  }

  private def lookupInNode2(node: ReaderNode, key: Key): Option[Element] = {
    node.level match {
      case 0 => {
        node.members.find(e => e.key == key)
      }
      case _ => find1(key, node.members) match {
        case Some(posLen) => {
          readNode(posLen) match {
            case Some(n) => lookupInNode2(n, key)
          }
        }
        case None => None
      }
    }
  }

  private def findStart(key: Key, members: List[Element]): Option[PosLen] = {
    members match {
      case List(p: KeyRef, KeyRef(k2, _, _), _*) if key < k2 => Option(PosLen(p.pos, Option(p.len)))
      case List(keyRef: KeyRef) => Option(PosLen(keyRef.pos, Option(keyRef.len)))
      case _ => find1(key, members)
    }
  }

  private  def find1(key: Key, members: List[Element]): Option[PosLen] = {
    members match {
      case List(KeyRef(k1, pos, len), KeyRef(k2, _, _), _*) if key >= k1 && key < k2 => Option(PosLen(pos, Option(len)))
      case List(KeyRef(k1, pos, len)) if key >= k1 => Option(PosLen(pos, Option(len)))
      case List(_, _*) => find1(key, members.tail)
      case List(_, _) => {
        members match {
          case List(KeyRef(k1, pos, len), KeyRef(k2, _, _), _*)  => {
            log.warning("find1, return None, key: %s, members[0].keyRef: %s, members[1].keyRef: %s".format(
              Utils.fromBytes(key.bytes), Utils.fromBytes(k1.bytes), Utils.fromBytes(k2.bytes)))
          }
        }
        None
      }
    }
  }

  private def recursiveFind(fromKey: Key, n: Int, childPos: PosLen): Option[PosLen] = {
    n match {
      case 1 => Option(childPos)
      case m if m > 1 => {
        readNode(childPos) match {
          case Some(childNode) => findLeafNode(fromKey, childNode, childPos)
          case None => None
        }
      }
    }
  }

  private def findLeafNode(fromKey: Key, node: ReaderNode, posLen: PosLen): Option[PosLen] = {
    if(node.level == 0) {
      return Option(posLen)
    }

    findStart(fromKey, node.members) match {
      case Some(childPosLen) => recursiveFind(fromKey, node.level, childPosLen)
      case None => None
    }
  }

  private def nextLeafNode(): Option[ReaderNode] = {
    val bytes = new Array[Byte](6)
    val read = this.randomAccessFile.read(bytes, 0, 6)
    if(read == 6) {
      readHeader(bytes) match {
        case (len: Long, _) if len == 0 => None
        case (len: Long, level: Int) if level == 0 => {
          val buf = new Array[Byte]((len - 2).toInt) // @TODO Long to Int
          this.randomAccessFile.read(buf)
          val entryList = Utils.decodeIndexNodes(buf, Compress(Constants.COMPRESS_PLAIN))
          Option(ReaderNode(level, entryList))
        }
        case (len: Long, level: Int) => {
          this.randomAccessFile.seek((len - 2).toInt) // @TODO Long to Int
          nextLeafNode
        }
      }
    } else {
      None
    }
  }

  private def findInLeaf(key: Key, members: List[Element]): Option[Element] = {
    log.debug(members.map(e => Utils.fromBytes(e.key.bytes)).mkString(","))
    members.find(p => p.key == key)
  }

  private def firstKey(node: ReaderNode): Key = {
    foldUntilStop((keyValue, _, _) => (Stop, (0, List(keyValue)), 0), (1, List.empty[Element]), 1, node.members) match {
      case (Stopped, (_, List(KeyValue(k: Key, _, _), _*)), _) => k
      case (Stopped, (_, List(KeyRef(k: Key, _, _), _*)), _) => k
    }
  }

  private def foldUntilStop(func: (Element, (Int, List[Value]), Int) => (FoldStatus, (Int, List[Value]), Int),
                            acc: (Int, List[Value]),
                            limit: Int,
                            members: List[Element]): (FoldStatus, (Int, List[Value]), Int) = {
    foldUntilStop2(func, (Continue, acc, limit), members)
  }

  private def foldUntilStop2(func: (Element, (Int, List[Value]), Int) => (FoldStatus, (Int, List[Value]), Int),
                             accWithStatus: (FoldStatus, (Int,  List[Value]), Int),
                             members: List[Element]): (FoldStatus, (Int, List[Value]), Int) = {
    accWithStatus match {
      case (Stop, result, limit) => (Stopped, result, limit)
      case (Continue, acc, limit) if members.length == 0 => (Ok, acc, limit)
      case (Continue, acc, limit) => foldUntilStop2(func, func(members.head, acc, limit), members.tail)
    }
  }

  def skip(n: Long): Unit = {
    randomAccessFile.seek(n)
  }

  def close(): Unit = {
    randomAccessFile.close
    log.debug("close, name: %s".format(this.name))
  }

  def delete(): Unit = {
    if(!file.delete) {
      log.warning("Failed to delete file: " + name)
    }
  }
}
