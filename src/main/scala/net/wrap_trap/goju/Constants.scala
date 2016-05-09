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
  type Value = Any

  val TOMBSTONE = Utils.toBytes("deleted")
  val FILE_FORMAT = "HAN2"

  val TAG_KV_DATA = 0x80.asInstanceOf[Byte]
  val TAG_DELETED = 0x81.asInstanceOf[Byte]
  val TAG_POSLEN = 0x82.asInstanceOf[Byte]
  val TAG_TRANSACT = 0x83.asInstanceOf[Byte]
  val TAG_KV_DATA2 = 0x84.asInstanceOf[Byte]
  val TAG_DELETED2 = 0x85.asInstanceOf[Byte]
}