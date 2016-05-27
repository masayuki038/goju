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
      assertKeyValue(ret.asInstanceOf[KeyValue], kv, now)
    }

    it("should encode a Tombstoned") {
      val now = new DateTime
      val tombstoned = new KeyValue(Utils.toBytes("hoge"), Constants.TOMBSTONE, Option(now))
      val ret = Utils.decodeIndexNode(Utils.encodeIndexNode(tombstoned))
      ret.isInstanceOf[KeyValue] should be(true)
      assertTombstoned(ret.asInstanceOf[KeyValue], tombstoned, now)
    }

    it("should encode a PosLen") {
      val posLen = new PosLen(Utils.toBytes("hoge"), Long.MaxValue, Int.MaxValue)
      val ret = Utils.decodeIndexNode(Utils.encodeIndexNode(posLen))
      ret.isInstanceOf[PosLen] should be(true)
      assertPosLen(ret.asInstanceOf[PosLen], posLen)
    }
  }

  describe("encodeIndexNodes") {
    it("should encode some elements") {
      val now = new DateTime
      val compress = Compress(Constants.COMPRESS_PLAIN)

      val kv = new KeyValue(Utils.toBytes("hoge"), "test", Option(now))
      val tombstoned = new KeyValue(Utils.toBytes("hoge"), Constants.TOMBSTONE, Option(now))
      val posLen = new PosLen(Utils.toBytes("hoge"), Long.MaxValue, Int.MaxValue)
      val target = List(kv, tombstoned, posLen)
      val packed = Utils.encodeIndexNodes(target, compress)
      val unpacked = Utils.decodeIndexNodes(packed, compress)

      unpacked(0).isInstanceOf[KeyValue] should be(true)
      assertKeyValue(unpacked(0).asInstanceOf[KeyValue], kv, now)

      unpacked(1).isInstanceOf[KeyValue] should be(true)
      assertTombstoned(unpacked(1).asInstanceOf[KeyValue], tombstoned, now)

      unpacked(2).isInstanceOf[PosLen] should be(true)
      assertPosLen(unpacked(2).asInstanceOf[PosLen], posLen)
    }
  }

  def assertKeyValue(target: KeyValue, expected: KeyValue, now: DateTime) = {
    target.key should be(expected.key)
    target.value should be(expected.value)
    target.timestamp().get.getMillis should be(now.getMillis / 1000L * 1000L)
  }

  def assertTombstoned(target: KeyValue, expected: KeyValue, now: DateTime) = {
    target.key should be(expected.key)
    target.tombstoned should be(true)
    target.timestamp().get.getMillis should be(now.getMillis / 1000L * 1000L)
  }

  def assertPosLen(target: PosLen, expected: PosLen) = {
    target.key should be(expected.key)
    target.pos should be(expected.pos)
    target.len should be(expected.len)
  }
}
