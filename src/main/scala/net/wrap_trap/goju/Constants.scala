package net.wrap_trap.goju

import java.nio.charset.Charset

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Constants {
  type Key = Array[Byte]
  type FilePos = (Int, Int)

  val TOMBSTONE = "deleted".getBytes(Charset.forName("UTF-8"))
}

sealed trait Value
final case class ExpValue(bytes: Array[Byte], expiry: Int = -1) extends Value
final case class FilePos(pos: Long, len: Int) extends Value