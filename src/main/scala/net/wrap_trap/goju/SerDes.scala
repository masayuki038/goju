package net.wrap_trap.goju

import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream}
import java.util.BitSet
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import msgpack4z._
import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.element.{PosLen, KeyValue, Element}
import org.joda.time.DateTime

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
      case posLen: PosLen => serialize(posLen)
    }
  }

  def deserialize(bytes: Array[Byte]): Element = {
    bytes(0) match {
      case TAG_KV_DATA => deserializeKeyValue(bytes, false)
      case TAG_KV_DATA2 => deserializeKeyValue(bytes, true)
      case TAG_DELETED => deserializeTombstoned(bytes, false)
      case TAG_DELETED2 => deserializeTombstoned(bytes, true)
      case TAG_POSLEN => deserializePosLen(bytes)
    }
  }

  def serializeByteArrayList(byteArrayList: List[Array[Byte]]): Array[Byte] = {
    import msgpack4z.CodecInstances.all._
    val binaryList = byteArrayList.map(byteArray => new Binary(byteArray))
    MsgpackCodec[List[Binary]].toBytes(binaryList, MsgOutBuffer.create())
  }

  def deserializeByteArrayList(serialized: Array[Byte]): List[Array[Byte]] = {
    import msgpack4z.CodecInstances.all._
    val binaryList = MsgpackCodec[List[Binary]].unpackAndClose(MsgInBuffer(serialized)) | List.empty[Binary]
    binaryList.map(binary => binary.value)
  }

  def serializeBloom(bloom: Bloom): Array[Byte] = {
    import msgpack4z.CodecInstances.all._
    val baos = new ByteArrayOutputStream
    using(new DataOutputStream(baos)) { out =>
      out.writeDouble(bloom.e)
      out.writeInt(bloom.n)
      out.writeInt(bloom.mb)
      val binaryList = bloom.a.map(bitmap => new Binary(bitmap.toByteArray))
      out.write(MsgpackCodec[List[Binary]].toBytes(binaryList, MsgOutBuffer.create()))
      baos.toByteArray
    }
  }

  def deserializeBloom(serialized: Array[Byte]): Bloom = {
    import msgpack4z.CodecInstances.all._
    using(new DataInputStream(new ByteArrayInputStream(serialized))) { in =>
      val e = in.readDouble
      val n = in.readInt
      val mb = in.readInt
      val buf = new Array[Byte](serialized.size - (8 + 4 + 4))
      in.read(buf)
      val binaryList = MsgpackCodec[List[Binary]].unpackAndClose(MsgInBuffer(buf)) | List.empty[Binary]
      val a = binaryList.map(binary => BitSet.valueOf(binary.value))
      new Bloom(e, n, mb, a)
    }
  }

  def gzipCompress(bytes: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    using(new GZIPOutputStream(baos)) { out =>
      out.write(bytes)
      baos.toByteArray
    }
  }

  def gzipDecompress(compressed: Array[Byte]): Array[Byte] = {
    val buf = new Array[Byte](8192)
    val bais = new ByteArrayInputStream(compressed)
    using(new GZIPInputStream(bais),  new ByteArrayOutputStream) { (in, out) =>
      Stream.continually(in.read(buf)).takeWhile(_ != -1).foreach(_ => out.write(buf))
      out.toByteArray
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
    using(new ElementOutputStream(baos)) { eos =>
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
    using(new ElementOutputStream(baos)) { eos =>
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

  private def serialize(posLen: PosLen): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    using(new ElementOutputStream(baos)) { eos =>
      eos.writeByte(TAG_POSLEN)
      eos.writeLong(posLen.pos)
      eos.writeInt(posLen.len)
      eos.write(posLen.key)
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

  private def deserializeKeyValue(body: Array[Byte], timestampReadable: Boolean): KeyValue = {
    val bais = new ByteArrayInputStream(body)
    using(new ElementInputStream(bais)) { eis =>
      val _ = eis.read
      val (timestamp, sizeOfTimestamp) = timestampReadable match {
        case true => (Option(new DateTime(eis.readTimestamp * 1000L)), SIZE_OF_TIMESTAMP)
        case _ => (None, 0)
      }
      val keyLen = eis.readInt
      val key = Key(eis.read(keyLen))
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
      val key = Key(eis.read(body.length - readSize))
      new KeyValue(key, TOMBSTONE, timestamp)
    }
  }

  private def deserializePosLen(body: Array[Byte]): PosLen = {
    val bais = new ByteArrayInputStream(body)
    using(new ElementInputStream(bais)) { eis =>
      val _ = eis.read
      val pos = eis.readLong
      val len = eis.readInt
      val readSize = SIZE_OF_ENTRY_TYPE + SIZE_OF_POS + SIZE_OF_LEN
      val key = Key(eis.read(body.length - readSize))
      new PosLen(key, pos, len)
    }
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