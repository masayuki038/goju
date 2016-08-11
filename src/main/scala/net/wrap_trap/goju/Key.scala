package net.wrap_trap.goju

import com.google.common.primitives.UnsignedBytes

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
case class Key(val bytes: Array[Byte]) {
  def ==(that: Key): Boolean = UnsignedBytes.lexicographicalComparator().compare(bytes, that.bytes) == 0
  def >(that: Key): Boolean = UnsignedBytes.lexicographicalComparator().compare(bytes, that.bytes) > 0
  def >=(that: Key): Boolean = UnsignedBytes.lexicographicalComparator().compare(bytes, that.bytes) >= 0
  def <(that: Key): Boolean = UnsignedBytes.lexicographicalComparator().compare(bytes, that.bytes) < 0
  def <=(that: Key): Boolean = UnsignedBytes.lexicographicalComparator().compare(bytes, that.bytes) <= 0
}

