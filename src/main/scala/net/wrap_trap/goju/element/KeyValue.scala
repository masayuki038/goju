package net.wrap_trap.goju.element

import org.joda.time.DateTime

import net.wrap_trap.goju.Constants.{Key, Value}
import net.wrap_trap.goju.Constants

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class KeyValue(val _key: Key, val _value: Value, val _timestamp: Option[DateTime] = None) extends Element {

  override def key(): Key = {
    this._key
  }

  def value(): Value = {
    this._value
  }

  override def expired(): Boolean = {
    this._timestamp.isDefined match {
      case true => (this._timestamp.get.getMillis < System.currentTimeMillis())
      case _ => false
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
      case i: Int => 5 + 4
      case bytes: Array[Byte] => bytes.length + 5 + 4
      case s: Symbol => 8 + 4
      case a: Any if(isTuple(a)) => 13 + 4
    }
    keySize + valueSize
  }

  def isTuple(x: Any): Boolean = {
    x.getClass.getName matches """^scala\.Tuple(\d+).*"""
  }
}
