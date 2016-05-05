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

  def estimateNodeSizeIncrement(key: Key, value: Value): Int = {
    val keySize = key.length
    val valueSize = value.asInstanceOf[Any] match {
      case ExpValue(i: Int, _) => 5 + 4
      case ExpValue(bytes: Array[Byte], _) => bytes.length + 5 + 4
      case ExpValue(s: Symbol, _) => 8 + 4
      case ExpValue(a: Any, _) if(isTuple(a)) => 13 + 4
    }
    keySize + valueSize
  }

  def isTuple(x: Any): Boolean = {
    x.getClass.getName matches """^scala\.Tuple(\d+).*"""
  }
}
