package net.wrap_trap.goju

import java.io.{File, ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.Charset
import java.util.zip.CRC32

import akka.actor.ActorSystem
import akka.event.{Logging, LogSource}
import com.google.common.primitives.UnsignedBytes
import com.typesafe.config.ConfigFactory
import net.wrap_trap.goju.element.Element
import net.wrap_trap.goju.Helper._
import org.joda.time.DateTime

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Utils {
  implicit val logSource: LogSource[AnyRef] = new GojuLogSource()
  val log = Logging(Utils.getActorSystem, this)

  lazy val getActorSystem = {
    ActorSystem("goju")
  }

  def ensureExpiry() = {
    if(!Settings.getSettings.hasPath("goju.expiry_secs")) {
      throw new IllegalStateException("config:goju.expiry_secs not found")
    }
  }

  def toBytes(str: String): Array[Byte] = {
    str.getBytes(Charset.forName("UTF-8"))
  }

  def fromBytes(bytes: Array[Byte]): String = {
    new String(bytes, Charset.forName("UTF-8"))
  }

  def to8Bytes(a: Long): Array[Byte] = {
    val bytes = new Array[Byte](8)
    bytes(7) = (0x000000ff & (a)).asInstanceOf[Byte]
    bytes(6) = (0x000000ff & (a >>> 8)).asInstanceOf[Byte]
    bytes(5) = (0x000000ff & (a >>> 16)).asInstanceOf[Byte]
    bytes(4) = (0x000000ff & (a >>> 24)).asInstanceOf[Byte]
    bytes(3) = (0x000000ff & (a >>> 32)).asInstanceOf[Byte]
    bytes(2) = (0x000000ff & (a >>> 40)).asInstanceOf[Byte]
    bytes(1) = (0x000000ff & (a >>> 48)).asInstanceOf[Byte]
    bytes(0) = (0x000000ff & (a >>> 56)).asInstanceOf[Byte]
    return bytes
  }

  def to4Bytes(a: Int): Array[Byte] = {
    val bytes = new Array[Byte](4)
    bytes(3) = (0x000000ff & (a)).asInstanceOf[Byte]
    bytes(2) = (0x000000ff & (a >>> 8)).asInstanceOf[Byte]
    bytes(1) = (0x000000ff & (a >>> 16)).asInstanceOf[Byte]
    bytes(0) = (0x000000ff & (a >>> 24)).asInstanceOf[Byte]
    return bytes
  }

  def to2Bytes(a: Int): Array[Byte] = {
    val bytes = new Array[Byte](2)
    bytes(1) =(0x000000ff & (a)).asInstanceOf[Byte]
    bytes(0) = (0x000000ff & (a >>> 8)).asInstanceOf[Byte]
    return bytes
  }

  def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    return UnsignedBytes.lexicographicalComparator().compare(a, b)
  }

  def encodeIndexNodes(elementList: List[Element],  compress: Compress): Array[Byte] = {
    var encoded = elementList.map { element => encodeIndexNode(element) }
    encoded = Array(0xff.asInstanceOf[Byte]) :: encoded
    val serialized = SerDes.serializeByteArrayList(encoded)
    compress.compress(serialized)
  }

  def decodeIndexNodes(packed: Array[Byte], compress: Compress): List[Element] = {
    val serialized = compress.decompress(packed)
    val encodedList = SerDes.deserializeByteArrayList(serialized)
    val endTag = encodedList.head
    if(endTag.length != 1 || endTag(0) != 0xff.asInstanceOf[Byte]) {
      throw new IllegalStateException("Invalid endTag")
    }
    encodedList.tail.map { bytes => decodeIndexNode(bytes) }
  }

  def encodeIndexNode(e: Element): Array[Byte] = {
    val body = SerDes.serialize(e)
    val baos = new ByteArrayOutputStream
    using(new ElementOutputStream(baos)) { eos =>
      eos.writeInt(body.length)
      eos.writeLong(getCRCValue(body))
      eos.write(body)
      eos.writeEndTag
      baos.toByteArray
    }
  }

  def decodeIndexNode(body: Array[Byte]): Element = {
    val bis = new ByteArrayInputStream(body)
    using(new ElementInputStream(bis)) { eis =>
      val bodyLen = eis.readInt
      val crc = eis.readLong
      val body = new Array[Byte](bodyLen)
      eis.read(body)
      eis.readEndTag

      if(!checkCRC(crc, body)) {
        throw new IllegalStateException("Invalid CRC: " + crc)
      }

      SerDes.deserialize(body)
    }
  }

  def toHexStrings(bytes: Array[Byte]): String = {
    val s = new StringBuilder
    for (b <- bytes) {
      s ++= (b & 0x000000ff).toInt.toHexString
      s += ' '
    }
    s.result
  }

  def dumpBinary(bytes: Array[Byte], subject: String): Unit = {
    val s = toHexStrings(bytes)
    log.debug(s"""$subject: $s""")
  }

  def decodeCRCData(logBinary: Array[Byte], broken: List[Array[Byte]], acc: List[Element]): List[Element] = {
    if(logBinary.size == 0) {
      if(broken.size > 0) {
        log.warning("Found " + broken.size + " broken logs in decodeCRCData")
      }
      return acc.reverse
    }
    val (crc, bin, rest) = parseBinaryLog(logBinary)
    if(!checkCRC(crc, bin)) {
      decodeCRCData(rest, bin :: broken, acc)
    } else {
      decodeCRCData(rest, broken, SerDes.deserialize(bin) :: acc)
    }
  }

  def expireTime(expireSecs: Int): DateTime = {
    new DateTime(System.currentTimeMillis + (expireSecs * 1000L))
  }

  def deleteFile(filePath: String): Unit = {
    val file = new File(filePath)
    if(file.exists && !file.delete()) {
      throw new IllegalStateException("Failed to delete: " + filePath)
    }
  }

  def renameFile(srcPath: String, destPath: String): Unit = {
    val src = new File(srcPath)
    val dest = new File(destPath)
    if(!src.renameTo(dest)) {
      throw new IllegalStateException("Failed to rename file. src: %s, dest: %s".format(srcPath, destPath))
    }
  }

  def btreeSize(level: Int): Int = {
    1 << level
  }

  private def parseBinaryLog(logBinary: Array[Byte]): (Long, Array[Byte], Array[Byte]) = {
    using(new ElementInputStream(new ByteArrayInputStream(logBinary))) { eis =>
      Utils.dumpBinary(logBinary, "logBinary in parseBinaryLog")
      val binSize = eis.readInt
      val crc = eis.readLong
      val bin = eis.read(binSize)
      eis.readEndTag
      val restSize =logBinary.size - (4 + 8 + binSize + 1)
      if(restSize > 0) {
        (crc, bin, eis.read(restSize))
      } else {
        (crc, bin, Array.empty[Byte])
      }
    }
  }

  private def getCRCValue(body: Array[Byte]): Long = {
    val crc32 = new CRC32
    crc32.update(body)
    crc32.getValue
  }

  private def checkCRC(crc: Long, body: Array[Byte]): Boolean  = {
    (crc == getCRCValue(body))
  }
}
