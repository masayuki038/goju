package net.wrap_trap.goju.element

import java.io.ByteArrayOutputStream
import java.math.BigInteger

import msgpack4z.MsgOutBuffer
import net.wrap_trap.goju.Helper._
import org.joda.time.DateTime

import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.Utils
import net.wrap_trap.goju.ElementOutputStream
import net.wrap_trap.goju.Constants
import net.wrap_trap.goju.Key

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
case class KeyValue(_key: Key, _value: Value, _timestamp: Option[DateTime]) extends Element {

  def this(_rawKey: Array[Byte], _value: Value, _timestamp: Option[DateTime] = None) {
    this(Key(_rawKey), _value, _timestamp)
  }

  override def key(): Key = {
    this._key
  }

  def value(): Value = {
    this._value
  }

  override def expired(): Boolean = {
    if (this._timestamp.isDefined) {
      this._timestamp.get.getMillis < System.currentTimeMillis()
    } else {
      false
    }
  }

  def timestamp(): Option[DateTime] = {
    this._timestamp
  }

  override def tombstoned(): Boolean = {
    this._value.equals(Constants.TOMBSTONE)
  }

  def estimateNodeSizeIncrement(): Int = {
    val keySize = _key.length
    val valueSize = _value.asInstanceOf[Any] match {
      case _: Int => 5 + 4
      case str: String => Utils.toBytes(str).length + 5 + 4
      case bytes: Array[Byte] => bytes.length + 5 + 4
      case _: Symbol => 8 + 4
      case a: Any if isTuple(a) => 13 + 4
    }
    keySize + valueSize
  }

  def isTuple(x: Any): Boolean = {
    x.getClass.getName.matches("""^scala\.Tuple(\d+).*""")
  }
}
