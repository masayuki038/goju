package net.wrap_trap.goju

import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.math.BigInteger

import msgpack4z.MsgOutBuffer
import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.element.{KeyValue, Element}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object SerDes {
  def serialize(entry: Element): Array[Byte] = {
    entry match {
      case kv: KeyValue => serialize(kv)
    }
  }

  def serialize(kv: KeyValue): Array[Byte] = {
    kv.tombstoned match {
      case true => serializeTombstoned(kv)
      case _ => serializeKeyValue(kv)
    }
  }

  private def serializeKeyValue(kv: KeyValue): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    using(new ElementOutputStream((baos))) { eos =>
      kv.timestamp match {
        case Some(ts) => {
          eos.writeByte(TAG_KV_DATA2)
          eos.writeTimestamp(ts.getMillis / 1000L)
        }
        case _ => eos.writeByte(TAG_KV_DATA)
      }
      eos.writeInt(kv.key.length)
      eos.write(kv.key)
      eos.write(serializeValue(kv.value))
      baos.toByteArray
    }
  }

  private def serializeTombstoned(tombstoned: KeyValue): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    using(new ElementOutputStream((baos))) { eos =>
      tombstoned.timestamp match {
        case Some(ts) => {
          eos.writeByte(TAG_DELETED2)
          eos.writeTimestamp(ts.getMillis / 1000L)
        }
        case _ => eos.writeByte(TAG_DELETED)
      }
      eos.write(tombstoned.key)
      baos.toByteArray
    }
  }

  private def serializeValue(value: Any): Array[Byte] = {
    val buf = MsgOutBuffer.create
    value match {
      case s: String => buf.packString(s)
      case i: Int => buf.packInt(i)
      case l: Long => buf.packLong(l)
      case d: Double => buf.packDouble(d)
      case f: Float => buf.packFloat(f)
      case bi: BigInteger => buf.packBigInteger(bi)
      case b: Byte => buf.packByte(b)
      case ba: Array[Byte] => buf.packBinary(ba)
      case bo: Boolean => buf.packBoolean(bo)
      case unsupported =>
        throw new IllegalArgumentException("Unsupported type of value: " + unsupported)
    }
    buf.result()
  }
}