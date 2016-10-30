package net.wrap_trap.goju

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
case class KeyRange(val fromKey: Key,
                    val fromInclude: Boolean,
                    val toKey: Option[Key],
                    val toInclude: Boolean,
                    val limit: Int) {
  def keyInFromRange(thatKey: Key): Boolean = {
    this.fromKey.bytes.length match {
      case 0 => true
      case _ => {
        if (fromInclude) {
          fromKey <= thatKey
        } else {
          fromKey < thatKey
        }
      }
    }
  }

  def keyInToRange(thatKey: Key): Boolean = {
    this.toKey match {
      case Some(to) => {
        if(toInclude) {
          to >= thatKey
        } else {
          to > thatKey
        }
      }
      case None => true
    }
  }
}
