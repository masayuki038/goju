package net.wrap_trap.goju

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.math.BigDecimal
import java.math.BigInteger

import msgpack4z.{MsgType, MsgInBuffer, MsgOutBuffer}
import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.element.{KeyValue, Element}
import org.joda.time.DateTime

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object SerDes {
  def deserialize(bytes: Array[Byte]): Element = {
    bytes(0) match {
      case TAG_KV_DATA => deserializeKeyValue(bytes, false)
      case TAG_KV_DATA2 => deserializeKeyValue(bytes, true)
      case TAG_DELETED => deserializeTombstoned(bytes, false)
      case TAG_DELETED2 => deserializeTombstoned(bytes, true)
    }
  }

  private def deserializeKeyValue(body: Array[Byte], timestampReadable: Boolean): KeyValue = {
    val bais = new ByteArrayInputStream(body)
    using(new ElementInputStream(bais)) { eis =>
      val _ = eis.read
      val (timestamp, sizeOfTimestamp) = timestampReadable match {
        case true => (Option(new DateTime(eis.readTimestamp * 1000L)), SIZE_OF_TIMESTAMP)
        case _ => (None, 0)
      }
      val keyLen = eis.readInt
      val key = eis.read(keyLen)
      val readSize = SIZE_OF_ENTRY_TYPE + sizeOfTimestamp + SIZE_OF_KEYSIZE + keyLen
      val value = eis.read(body.length - readSize)
      new KeyValue(key, deserializeValue(value), timestamp)
    }
  }

  private def deserializeTombstoned(body: Array[Byte],  timestampReadable: Boolean): KeyValue = {
    val bais = new ByteArrayInputStream(body)
    using(new ElementInputStream(bais)) { eis =>
      val _ = eis.read
      val (timestamp, sizeOfTimestamp) = timestampReadable match {
        case true => (Option(new DateTime(eis.readTimestamp * 1000L)), SIZE_OF_TIMESTAMP)
        case _ => (None, 0)
      }
      val readSize = SIZE_OF_ENTRY_TYPE + sizeOfTimestamp
      val key = eis.read(body.length - readSize)
      new KeyValue(key, TOMBSTONE, timestamp)
    }
  }


  def serialize(entry: Element): Array[Byte] = {
    entry match {
      case kv: KeyValue => serialize(kv)
    }
  }

  private def serialize(kv: KeyValue): Array[Byte] = {
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
      case d: Double => buf.packDouble(d)
      case ba: Array[Byte] => buf.packBinary(ba)
      case bo: Boolean => buf.packBoolean(bo)
      case unsupported =>
        throw new IllegalArgumentException("Unsupported type of value: " + unsupported)
    }
    buf.result()
  }

  private def deserializeValue(bytes: Array[Byte]): Any = {
    val buf = MsgInBuffer(bytes)
    val nextType = buf.nextType
    nextType match {
      case MsgType.STRING => buf.unpackString
      case MsgType.INTEGER => buf.unpackInt
      case MsgType.FLOAT => buf.unpackDouble
      case MsgType.BINARY => buf.unpackBinary
      case MsgType.BOOLEAN => buf.unpackBoolean
      case _ =>
        throw new IllegalArgumentException("Unsupported type of value: " + nextType)
    }
  }
}