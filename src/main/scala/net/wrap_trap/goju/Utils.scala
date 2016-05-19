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

  // delete after
  def hasExpired(ts: DateTime): Boolean = {
    ts.isBefore(DateTime.now)
  }

  def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    return UnsignedBytes.lexicographicalComparator().compare(a, b)
  }

//  def encodeIndexNodes(entryList: List[(Key, Value)],  final Compress compress): Array[Byte] = {
//    List<byte[]> encoded = entryList.stream()
//      .map((entry) -> encodeIndexNode(entry, compress)).collect(Collectors.toList());
//    encoded.add(0, new byte[]{(byte)0xFF});
//    byte[] serialized = serializeEncodedEntryList(encoded);
//    return compress.compress(serialized);
//  }

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
