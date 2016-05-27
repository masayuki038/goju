package net.wrap_trap.goju

import net.wrap_trap.goju.Constants._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */

case class Compress(var method: Byte) {

  def compress(plain: Array[Byte]): Array[Byte] = {
    val body = method match {
      case COMPRESS_PLAIN => plain
      case _ => throw new IllegalArgumentException("Unsupported compress type: " + method)
    }

    val packed = new Array[Byte](body.length + 1)
    packed(0) = COMPRESS_PLAIN
    Array.copy(body, 0, packed, 1, body.length)
    packed
  }

  def decompress(packed: Array[Byte]): Array[Byte] = {
    val compressedBy = packed(0)
    val body = new Array[Byte](packed.length - 1)
    Array.copy(packed, 1, body, 0, packed.length - 1)
    compressedBy match {
      case COMPRESS_PLAIN => body
      case _ => throw new IllegalArgumentException("Unsupported compress type: " + compressedBy)
    }
  }
}
