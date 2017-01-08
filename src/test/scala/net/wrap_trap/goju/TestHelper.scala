package net.wrap_trap.goju

import java.io.File

import akka.event.{LogSource, Logging}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object TestHelper {
  implicit val logSource: LogSource[AnyRef] = new GojuLogSource()
  val log = Logging(Utils.getActorSystem, this)

  def deleteDirectory(file: File): Unit = {
    if(file.isDirectory) {
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteDirectory(_))
    }
    if(file.exists && !file.delete) {
      file.listFiles.foreach(f => log.info("remain: " + f.getAbsolutePath))
      throw new IllegalStateException("Failed to delete: " + file.getName)
    }
  }
}
