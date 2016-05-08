package net.wrap_trap.goju.element

import org.joda.time.DateTime

import net.wrap_trap.goju.Constants.Key

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
trait Element {

  def key(): Key

  def estimateNodeSizeIncrement(): Int

  def expired() = false

  def tombstoned() = false


}
