package net.wrap_trap.goju

import java.io.{File, FileInputStream, BufferedInputStream}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object SequentialReader {
  def open(name: String): SequentialReader = {
    new SequentialReader(name)
  }

  def serialize(sequentialReader: SequentialReader): (SequentialReader, Long) = {
    (sequentialReader, sequentialReader.inputStream.pointer)
  }

  def deserialize(serialized: (SequentialReader, Long)): SequentialReader = {
    val (reader, pos) = serialized
    val newReader = SequentialReader.open(reader.name)
    newReader.skip(pos)
    newReader
  }
}

class SequentialReader(val name: String) extends Reader {
  val inputStream = buildInputStream(name)
  val file = new File(name)

  private def buildInputStream(name: String): ElementInputStream = {
    val settings = Settings.getSettings
    val bufferPoolSize = settings.getInt("read_buffer_size", 524288)
    new ElementInputStream(new BufferedInputStream(new FileInputStream(name), bufferPoolSize))
  }

  def skip(n: Long): Unit = {
    inputStream.skip(n)
  }

  def close(): Unit = {
   inputStream.close
  }

  def delete(): Unit = {
    if(!file.delete) {
      log.warn("Failed to delete file: " + name)
    }
  }
}
