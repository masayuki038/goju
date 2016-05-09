package net.wrap_trap.goju

import java.io.{DataOutputStream, OutputStream}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class ElementOutputStream(os: OutputStream) extends AutoCloseable {

  val internal = new DataOutputStream(os)

  def writeShort(s: Short) = {
    this.internal.writeShort(s)
  }

  def writeInt(i: Int) = {
    this.internal.writeInt(i)
  }

  def writeTimestamp(l: Long) = {
    // write last 4bytes
    this.internal.writeInt(l.asInstanceOf[Int])
  }

  def writeLong(l: Long) = {
    this.internal.writeLong(l)
  }

  def writeByte(b: Byte) = {
    this.internal.writeByte(b)
  }

  def write(bytes: Array[Byte]) = {
    this.internal.write(bytes)
  }

  def writeEndTag() = {
    writeByte(0xFF.asInstanceOf[Byte])
  }

  def close() = {
    try {
      this.internal.close
    } catch {
      case e: Exception => ???
    }
  }
}
