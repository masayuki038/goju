package net.wrap_trap.goju

import java.io.{InputStream, IOException, EOFException, DataInputStream}

import com.google.common.primitives.UnsignedInteger

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class ElementInputStream(is: InputStream) extends AutoCloseable {
  val internal = new DataInputStream(is)

  def read(): Int = {
    this.internal.read()
  }

  def read(bytes: Array[Byte]): Int = {
    this.internal.read(bytes)
  }

  def readInt(): Int = {
    this.internal.readInt()
  }

  def readTimestamp(): Long = {
    UnsignedInteger.fromIntBits(this.internal.readInt()).longValue()
  }

  def readLong(): Long = {
    this.internal.readLong()
  }

  def read(size: Int): Array[Byte] = {
    val buf = new Array[Byte](size)
    val read = this.internal.read(buf)
    if (read < size)
      throw new EOFException(s"""read: $read, size: $size""")
    buf
  }

  def readEndTag() = {
    val ch1 = this.internal.read()
    if (ch1 < 0)
      throw new EOFException
    if (ch1 != 0xFF)
      throw new IllegalStateException("endTag != 0xFF. endTag: " + ch1)
  }

  override def close() = {
    try {
      this.internal.close();
    } catch {
      case ignore: IOException => {}
    }
  }
}
