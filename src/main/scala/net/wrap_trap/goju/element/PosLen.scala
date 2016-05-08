package net.wrap_trap.goju.element

import net.wrap_trap.goju.Constants.Key


/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class PosLen(val _key: Key, val _pos: Long, _len: Int) extends Element {

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
