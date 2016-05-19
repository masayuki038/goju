package net.wrap_trap.goju

import net.wrap_trap.goju.element.{PosLen, KeyValue}
import org.joda.time.DateTime
import org.scalatest.{FunSpec, BeforeAndAfter, Matchers, FlatSpec}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class UtilsSpec extends FunSpec with Matchers with BeforeAndAfter {

  describe("now + 1min")  {
    it("should not be expired") {
      Utils.hasExpired(DateTime.now.plusMinutes(1)) should equal(false)
    }
  }

  describe("now - 1min") {
    it("should be expired") {
      Utils.hasExpired(DateTime.now.plusMinutes(-1)) should equal(true)
    }
  }

  describe("estimateNodeSizeIncrement") {
    describe("when the value is Int") {
      it("should return the estimate size for adding a node") {
        new KeyValue(Utils.toBytes("test"), 1).estimateNodeSizeIncrement should equal(13)
      }
    }

    describe("when the value is bytes") {
      it("should return the estimate size for adding a node") {
        new KeyValue(Utils.toBytes("test"), Utils.toBytes("foo")).estimateNodeSizeIncrement should equal(16)
      }
    }

    describe("when the value is Symbol") {
      it("should return the estimate size for adding a node") {
        new KeyValue(Utils.toBytes("test"), 'foo).estimateNodeSizeIncrement should equal(16)
      }
    }

    describe("when the value is Tuple") {
      it("should return the esitmate size for adding a node") {
        new KeyValue(Utils.toBytes("test"), ("foobar", 2)).estimateNodeSizeIncrement should equal(21)
      }
    }
  }

  describe("encodeIndexNode") {
    it("should encode a KeyValue") {
      val now = new DateTime
      val kv = new KeyValue(Utils.toBytes("hoge"), "test", Option(now))
      val ret = Utils.decodeIndexNode(Utils.encodeIndexNode(kv))
      ret.isInstanceOf[KeyValue] should be(true)
      val kv2 = ret.asInstanceOf[KeyValue]
      kv2.key should be(kv.key)
      kv2.value should be(kv.value)
      kv2.timestamp().get.getMillis should be(now.getMillis / 1000L * 1000L)
    }

    it("should encode a Tombstoned") {
      val now = new DateTime
      val tombstoned = new KeyValue(Utils.toBytes("hoge"), Constants.TOMBSTONE, Option(now))
      val ret = Utils.decodeIndexNode(Utils.encodeIndexNode(tombstoned))
      ret.isInstanceOf[KeyValue] should be(true)
      val tombstoned2 = ret.asInstanceOf[KeyValue]
      tombstoned2.key should be(tombstoned.key)
      tombstoned2.tombstoned should be(true)
      tombstoned2.timestamp().get.getMillis should be(now.getMillis / 1000L * 1000L)
    }

    it("should encode a PosLen") {
      val posLen = new PosLen(Utils.toBytes("hoge"), Long.MaxValue, Int.MaxValue)
      val ret = Utils.decodeIndexNode(Utils.encodeIndexNode(posLen))
      ret.isInstanceOf[PosLen] should be(true)
      val posLen2 = ret.asInstanceOf[PosLen]
      posLen2.key should be(posLen.key)
      posLen2.pos should be(posLen.pos)
      posLen2.len should be(posLen.len)
    }
  }
}
