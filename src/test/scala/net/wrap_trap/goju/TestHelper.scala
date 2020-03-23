package net.wrap_trap.goju

import java.io.File

import akka.event.{LogSource, Logging}
import org.slf4j.LoggerFactory

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object TestHelper {
  val log = LoggerFactory.getLogger(this.getClass)

  def deleteDirectory(file: File): Unit = {
    if(file.isDirectory) {
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteDirectory(_))
    }
    if(file.exists && !file.delete) {
      val dirName = file.getName
      log.error("failed to delete %s".format(dirName))
      throw new IllegalStateException("Failed to delete: " + dirName)
    }
  }

  def remakeDir(file: File): Unit = {
    if(!file.exists) {
      deleteDirectory(file)
      file.mkdir()
    }
  }
}
