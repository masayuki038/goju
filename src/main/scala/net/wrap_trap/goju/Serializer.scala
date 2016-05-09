package net.wrap_trap.goju

import java.io.ByteArrayOutputStream

import net.wrap_trap.goju.Constants._
import net.wrap_trap.goju.Helper._
import net.wrap_trap.goju.element.{KeyValue, Element}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object Serializer {

}

abstract class Serializer {
  def serialize(entry: Element): Array[Byte]
}

class KeyValueSerializer extends Serializer {
  override def serialize(entry: Element): Array[Byte] = {
    entry match {
      case kv: KeyValue => toBytes(kv)
    }
  }

  def toBytes(kv: KeyValue): Array[Byte] = {
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
      //eos.write(kv.value.to)
      baos.toByteArray
    }
  }
}