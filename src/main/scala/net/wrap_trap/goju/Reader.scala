package net.wrap_trap.goju

import java.io.ByteArrayInputStream

import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.element.Element
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
trait Reader {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def skip(n: Long): Unit

  def destroy(): Unit = {
    close()
    delete()
  }

  def close(): Unit
  def delete(): Unit

  protected def readHeader(bytes: Array[Byte]): (Long, Int) = {
    using(new ElementInputStream(new ByteArrayInputStream(bytes))) { eis =>
      (eis.readInt().toLong, eis.readShort().toInt)
    }
  }
}

case class ReaderNode(level: Int, members: List[Element] = List.empty)
