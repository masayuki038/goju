package net.wrap_trap.goju

import java.nio.charset.Charset

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Constants {
  type Value = Any

  val TOMBSTONE = Utils.toBytes("deleted")
  val FILE_FORMAT = "HAN2"
  val FIRST_BLOCK_POS: Long = FILE_FORMAT.getBytes.size

  val TAG_KV_DATA = 0x80.asInstanceOf[Byte]
  val TAG_DELETED = 0x81.asInstanceOf[Byte]
  val TAG_POSLEN = 0x82.asInstanceOf[Byte]
  val TAG_TRANSACT = 0x83.asInstanceOf[Byte]
  val TAG_KV_DATA2 = 0x84.asInstanceOf[Byte]
  val TAG_DELETED2 = 0x85.asInstanceOf[Byte]

  val SIZE_OF_ENTRY_TYPE = 1
  val SIZE_OF_KEYSIZE = 4
  val SIZE_OF_TIMESTAMP = 4
  val SIZE_OF_POS = 8
  val SIZE_OF_LEN = 4

  val COMPRESS_PLAIN = 0x00.asInstanceOf[Byte]
}