package net.wrap_trap.goju.element

import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.Key

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
case class KeyRef(val _key: Key, val _pos: Long, _len: Int) extends Element {

  def this(_rawKey: Array[Byte], _pos: Long, _len: Int) = {
    this(Key(_rawKey), _pos, _len)
  }

  override def key(): Key = {
    this._key
  }

  def pos(): Long = {
    this._pos
  }

  def len(): Int = {
    this._len
  }

  def estimateNodeSizeIncrement(): Int = {
    _key.length + 5 + 4
  }
}
