package net.wrap_trap.goju

import net.wrap_trap.goju.element.KeyValue
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, Matchers, FunSpec}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class SerDesSpec extends FunSpec with Matchers with BeforeAndAfter {

  describe("Int value") {
    it("should not be serialize and deserialize") {
      testKeyValue(new KeyValue(Utils.toBytes("Int Value"), Int.MaxValue))
      testKeyValue(new KeyValue(Utils.toBytes("Int Value"), Int.MinValue))
      testKeyValueWithTimestamp(new KeyValue(Utils.toBytes("Int Value"), Int.MaxValue, Option(new DateTime())))
      testKeyValueWithTimestamp(new KeyValue(Utils.toBytes("Int Value"), Int.MinValue, Option(new DateTime())))
    }
  }

  describe("Double value") {
    it("should not be serialize and deserialize") {
      testKeyValue(new KeyValue(Utils.toBytes("Double Value"), Double.MaxValue))
      testKeyValue(new KeyValue(Utils.toBytes("Double Value"), Double.MinValue))
      testKeyValueWithTimestamp(new KeyValue(Utils.toBytes("Double Value"), Double.MaxValue, Option(new DateTime())))
      testKeyValueWithTimestamp(new KeyValue(Utils.toBytes("Double Value"), Double.MinValue, Option(new DateTime())))
    }
  }

  describe("String value") {
    it("should not be serialize and deserialize") {
      testKeyValue(new KeyValue(Utils.toBytes("String Value"), "＃１２3"))
      testKeyValueWithTimestamp(new KeyValue(Utils.toBytes("String Value"), "＃１２3", Option(new DateTime())))
    }
  }

  describe("Binary value") {
    it("should not be serialize and deserialize") {
      testKeyValue(new KeyValue(Utils.toBytes("Binary Value"), Array[Byte](0x01, 0x02, 0x3)))
      testKeyValueWithTimestamp(new KeyValue(
        Utils.toBytes("Binary Value"), Array[Byte](0x01, 0x02, 0x3), Option(new DateTime())))
    }
  }

  describe("Boolean value") {
    it("should not be serialize and deserialize") {
      testKeyValue(new KeyValue(Utils.toBytes("Boolean Value"), true))
      testKeyValue(new KeyValue(Utils.toBytes("Boolean Value"), false))
      testKeyValueWithTimestamp(new KeyValue(Utils.toBytes("Boolean Value"), true, Option(new DateTime())))
      testKeyValueWithTimestamp(new KeyValue(Utils.toBytes("Boolean Value"), false, Option(new DateTime())))
    }
  }

  describe("Tombstoned value") {
    it("should not be serialize and deserialize") {
      testKeyValue(new KeyValue(Utils.toBytes("Tombstoned Value"), Constants.TOMBSTONE), true)
      testKeyValueWithTimestamp(
        new KeyValue(Utils.toBytes("Tombstoned Value"), Constants.TOMBSTONE, Option(new DateTime())), true)
    }
  }

  def testKeyValueInternal(kv: KeyValue, tombstoned:Boolean): KeyValue = {
    val ret = SerDes.deserialize(SerDes.serialize(kv))
    ret.isInstanceOf[KeyValue] should be(true)
    val kv2 = ret.asInstanceOf[KeyValue]
    kv2.key should be(kv.key)
    kv2.value should be(kv.value)
    kv2.tombstoned should be(tombstoned)
    kv2
  }

  def testKeyValue(kv: KeyValue, tombstoned:Boolean = false): KeyValue = {
    val kv2 = testKeyValueInternal(kv, tombstoned)
    kv2.timestamp.isEmpty should be(true)
    kv2
  }

  def testKeyValueWithTimestamp(kv: KeyValue, tombstoned:Boolean = false) = {
    val kv2 = testKeyValueInternal(kv, tombstoned)
    kv2.timestamp().isDefined should be(true)
    kv2.timestamp.get.getMillis / 1000 should be(kv.timestamp.get.getMillis / 1000)
  }
}
