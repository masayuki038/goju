package net.wrap_trap.goju

import java.io.ByteArrayInputStream

import com.typesafe.scalalogging.Logger
import net.wrap_trap.goju.Helper._
import org.slf4j.LoggerFactory
import net.wrap_trap.goju.element.{Element, KeyValue, KeyRef}

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
trait Reader {
  val log = Logger(LoggerFactory.getLogger(this.getClass))

  def skip(n: Long): Unit

  def destroy() = {
    close
    delete
  }

  def close(): Unit
  def delete(): Unit


  protected def readHeader(bytes: Array[Byte]): (Long, Int) = {
    using(new ElementInputStream(new ByteArrayInputStream(bytes))) { eis =>
      (eis.readInt.toLong, eis.readShort.toInt)
    }
  }
}

case class ReaderNode(level: Int, members: List[Element] = List.empty)

sealed abstract class FoldResult
case object Done extends FoldResult

sealed abstract class FoldStatus
case object Stop extends FoldStatus
case object Stopped extends FoldStatus
case object Continue extends FoldStatus
case object Ok extends FoldStatus