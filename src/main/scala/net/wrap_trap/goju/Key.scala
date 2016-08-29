package net.wrap_trap.goju

import com.google.common.primitives.UnsignedBytes._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
case class Key(val bytes: Array[Byte]) {
  def ==(that: Key): Boolean = lexicographicalComparator().compare(bytes, that.bytes) == 0
  def >(that: Key): Boolean = lexicographicalComparator().compare(bytes, that.bytes) > 0
  def >=(that: Key): Boolean = lexicographicalComparator().compare(bytes, that.bytes) >= 0
  def <(that: Key): Boolean = lexicographicalComparator().compare(bytes, that.bytes) < 0
  def <=(that: Key): Boolean = lexicographicalComparator().compare(bytes, that.bytes) <= 0

  override def hashCode(): Int = bytes.hashCode

  override def equals(that: Any): Boolean = {
    that match {
      case thatKey: Key => this == thatKey
      case _ => super.equals(that)
    }
  }
}
