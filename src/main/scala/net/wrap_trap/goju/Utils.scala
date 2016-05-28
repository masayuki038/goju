package net.wrap_trap.goju

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.Charset
import java.util.zip.CRC32

import com.google.common.primitives.UnsignedBytes
import net.wrap_trap.goju.Constants.Key
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
  def toBytes(str: String): Array[Byte] = {
    str.getBytes(Charset.forName("UTF-8"))
  }

  def to4Bytes(a: Int): Array[Byte] = {
    val bytes = Array[Byte](4)
    bytes(3) =(0x000000ff & (a)).asInstanceOf[Byte]
    bytes(2) = (0x000000ff & (a >>> 8)).asInstanceOf[Byte]
    bytes(1) = (0x000000ff & (a >>> 16)).asInstanceOf[Byte]
    bytes(0) = (0x000000ff & (a >>> 24)).asInstanceOf[Byte]
    return bytes
  }

  def to2Bytes(a: Int): Array[Byte] = {
    val bytes = Array[Byte](2)
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

  private def getCRCValue(body: Array[Byte]): Long = {
    val crc32 = new CRC32
    crc32.update(body)
    crc32.getValue
  }

  private def checkCRC(crc: Long, body: Array[Byte]): Boolean  = {
    (crc == getCRCValue(body))
  }
}
