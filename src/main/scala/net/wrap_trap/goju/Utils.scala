package net.wrap_trap.goju

import java.nio.charset.Charset

import com.google.common.primitives.UnsignedBytes
import net.wrap_trap.goju.Constants.Key
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

}
