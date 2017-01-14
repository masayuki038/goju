package net.wrap_trap.goju

import java.io.ByteArrayInputStream

import akka.event.{Logging, LogSource}
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.element.Element

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
trait Reader {
  implicit val logSource: LogSource[AnyRef] = new GojuLogSource()
  val log = Logging(Utils.getActorSystem, this)

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
