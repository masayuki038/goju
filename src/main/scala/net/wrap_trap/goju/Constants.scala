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
  type Value = Array[Byte]
  type Expiry = Int
  type FilePos = (Int, Int)

  val TOMBSTONE = "deleted".getBytes(Charset.forName("UTF-8"))

}


class ExpValue[T]

object ExpValue {

  implicit object ExpirableValue extends ExpValue[(Constants.Expiry, Constants.Value)]

  implicit object Value extends ExpValue[Constants.Value]

  implicit object FilePos extends ExpValue[Constants.FilePos]

}