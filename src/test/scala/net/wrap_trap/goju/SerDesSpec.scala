package net.wrap_trap.goju

import net.wrap_trap.goju.element.KeyRef
import net.wrap_trap.goju.element.KeyValue
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers
import org.scalatest.FunSpec

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
class SerDesSpec extends FunSpec with Matchers with BeforeAndAfter {

  describe("Int value") {
    it("should be same content after serializing and deserializing") {
      testKeyValue(new KeyValue(Utils.toBytes("Int Value"), Int.MaxValue))
      testKeyValue(new KeyValue(Utils.toBytes("Int Value"), Int.MinValue))
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("Int Value"), Int.MaxValue, Option(new DateTime())))
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("Int Value"), Int.MinValue, Option(new DateTime())))
    }
  }

  describe("Double value") {
    it("should be same content after serializing and deserializing") {
      testKeyValue(new KeyValue(Utils.toBytes("Double Value"), Double.MaxValue))
      testKeyValue(new KeyValue(Utils.toBytes("Double Value"), Double.MinValue))
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("Double Value"), Double.MaxValue, Option(new DateTime())))
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("Double Value"), Double.MinValue, Option(new DateTime())))
    }
  }

  describe("String value") {
    it("should be same content after serializing and deserializing") {
      testKeyValue(new KeyValue(Utils.toBytes("String Value"), "＃１２3"))
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("String Value"), "＃１２3", Option(new DateTime())))
    }
  }

  describe("Binary value") {
    it("should be same content after serializing and deserializing") {
      testKeyValue(new KeyValue(Utils.toBytes("Binary Value"), Array[Byte](0x01, 0x02, 0x3)))
      testKeyValueWithTimestamp(
        new KeyValue(
          Utils.toBytes("Binary Value"),
          Array[Byte](0x01, 0x02, 0x3),
          Option(new DateTime())))
    }
  }

  describe("Boolean value") {
    it("should be same content after serializing and deserializing") {
      testKeyValue(new KeyValue(Utils.toBytes("Boolean Value"), true))
      testKeyValue(new KeyValue(Utils.toBytes("Boolean Value"), false))
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("Boolean Value"), true, Option(new DateTime())))
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("Boolean Value"), false, Option(new DateTime())))
    }
  }

  describe("Tombstoned value") {
    it("should be same content after serializing and deserializing") {
      testKeyValue(
        new KeyValue(Utils.toBytes("Tombstoned Value"), Constants.TOMBSTONE),
        tombstoned = true)
      testKeyValueWithTimestamp(
        new KeyValue(
          Utils.toBytes("Tombstoned Value"),
          Constants.TOMBSTONE,
          Option(new DateTime())),
        tombstoned = true)
    }
  }

  describe("PosLen") {
    it("should be same content after serializing and deserializing") {
      val posLen = new KeyRef(Utils.toBytes("PosLen"), Long.MaxValue, Int.MaxValue)
      val ret = SerDes.deserialize(SerDes.serialize(posLen))
      ret.isInstanceOf[KeyRef] should be(true)
      val posLen2 = ret.asInstanceOf[KeyRef]
      posLen2.key() should be(posLen.key())
      posLen2.pos() should be(posLen.pos())
      posLen2.len() should be(posLen.len())
    }
  }

  describe("Bloom") {
    it("should be returned same results after Ser/Des") {
      val bloom = new Bloom(10)
      bloom.add(Key("test".getBytes))
      bloom.member(Key("test".getBytes)) should equal(true)
      bloom.member(Key("test2".getBytes)) should equal(false)
    }
  }

  def testKeyValueInternal(kv: KeyValue, tombstoned: Boolean): KeyValue = {
    val ret = SerDes.deserialize(SerDes.serialize(kv))
    ret.isInstanceOf[KeyValue] should be(true)
    val kv2 = ret.asInstanceOf[KeyValue]
    kv2.key should be(kv.key())
    kv2.value should be(kv.value())
    kv2.tombstoned should be(tombstoned)
    kv2
  }

  def testKeyValue(kv: KeyValue, tombstoned: Boolean = false): KeyValue = {
    val kv2 = testKeyValueInternal(kv, tombstoned)
    kv2.timestamp().isEmpty should be(true)
    kv2
  }

  private def testKeyValueWithTimestamp(kv: KeyValue, tombstoned: Boolean = false): Unit = {
    val kv2 = testKeyValueInternal(kv, tombstoned)
    kv2.timestamp().isDefined should be(true)
    kv2.timestamp().get.getMillis / 1000 should be(kv.timestamp().get.getMillis / 1000)
  }
}
