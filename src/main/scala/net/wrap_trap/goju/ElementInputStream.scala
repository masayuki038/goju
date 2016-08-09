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
  var p = 0L

  def read(): Int = {
    p += 1
    this.internal.read()
  }

  def read(bytes: Array[Byte]): Int = {
    p += bytes.length
    this.internal.read(bytes)
  }

  def readShort(): Short = {
    p += 2
    this.internal.readShort
  }

  def readInt(): Int = {
    p += 4
    this.internal.readInt
  }

  def readTimestamp(): Long = {
    p += 4
    UnsignedInteger.fromIntBits(this.internal.readInt).longValue
  }

  def readLong(): Long = {
    p += 8
    this.internal.readLong
  }

  def read(size: Int): Array[Byte] = {
    p += size
    val buf = new Array[Byte](size)
    val read = this.internal.read(buf)
    if (read < size)
      throw new EOFException(s"""read: $read, size: $size""")
    buf
  }

  def readEndTag() = {
    val ch1 = read
    if (ch1 < 0)
      throw new EOFException
    if (ch1 != 0xFF)
      throw new IllegalStateException("endTag != 0xFF. endTag: " + ch1)
  }

  def pointer() : Long = p

  def skip(n: Long) = {
    this.internal.skip(n)
  }

  override def close() = {
    try {
      this.internal.close
      p = 0L
    } catch {
      case ignore: IOException => {}
    }
  }
}
