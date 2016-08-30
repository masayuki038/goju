package net.wrap_trap.goju

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
case class KeyRange(val fromKey: Key,
                    val fromInclude: Boolean,
                    val toKey: Key,
                    val toInclude: Boolean,
                    val limit: Int) {
  def keyInFromRange(thatKey: Key): Boolean = {
    if(fromInclude) {
      fromKey <= thatKey
    } else {
      fromKey < thatKey
    }
  }

  def keyInToRange(thatKey: Key): Boolean = {
    if(toInclude) {
      toKey >= thatKey
    } else {
      toKey > thatKey
    }
  }
}
