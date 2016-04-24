package net.wrap_trap.goju

import java.nio.charset.Charset

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Utils {
  def toBytes(str: String): Array[Byte] = {
    str.getBytes(Charset.forName("UTF-8"));
  }
}