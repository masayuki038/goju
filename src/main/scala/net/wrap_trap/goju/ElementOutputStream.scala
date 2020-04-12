package net.wrap_trap.goju

import java.io.DataOutputStream
import java.io.OutputStream

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
class ElementOutputStream(os: OutputStream) extends AutoCloseable {

  val internal = new DataOutputStream(os)
  var p = 0L

  def writeShort(s: Short): Unit = {
    p += 2
    this.internal.writeShort(s)
  }

  def writeInt(i: Int): Unit = {
    p += 4
    this.internal.writeInt(i)
  }

  def writeTimestamp(l: Long): Unit = {
    p += 4
    // write last 4bytes
    this.internal.writeInt(l.asInstanceOf[Int])
  }

  def writeLong(l: Long): Unit = {
    p += 8
    this.internal.writeLong(l)
  }

  def writeByte(b: Byte): Unit = {
    p += 1
    this.internal.writeByte(b)
  }

  def write(bytes: Array[Byte]): Unit = {
    p += bytes.length
    this.internal.write(bytes)
  }

  def writeEndTag(): Unit = {
    writeByte(0xFF.asInstanceOf[Byte])
  }

  def pointer(): Long = p

  def close(): Unit = {
    try {
      this.internal.close()
      p = 0L
    } catch {
      case _: Exception =>
    }
  }
}
