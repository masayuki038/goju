package net.wrap_trap.goju

import java.io.File

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object TestHelper {
  def deleteDirectory(file: File): Unit = {
    if(file.isDirectory) {
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteDirectory(_))
    }
    if(!file.delete) {
      throw new IllegalStateException("Failed to delete: " + file.getName)
    }
  }
}
