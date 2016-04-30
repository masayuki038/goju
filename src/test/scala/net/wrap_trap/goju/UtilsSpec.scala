package net.wrap_trap.goju

import org.joda.time.DateTime
import org.scalatest.{FunSpec, BeforeAndAfter, Matchers, FlatSpec}

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class UtilsSpec  extends FunSpec with Matchers with BeforeAndAfter {

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
        Utils.estimateNodeSizeIncrement(Utils.toBytes("test"), ExpValue(1)) should equal(13)
      }
    }
    describe("when the value is bytes") {
      it("should return the estimate size for adding a node") {
        Utils.estimateNodeSizeIncrement(Utils.toBytes("test"), ExpValue(Utils.toBytes("foo"))) should equal(16)
      }
    }
    describe("when the value is Symbol") {
      it("should return the estimate size for adding a node") {
        Utils.estimateNodeSizeIncrement(Utils.toBytes("test"), ExpValue('foo)) should equal(16)
      }
    }
    describe("when the value is Tuple") {
      it("should return the esitmate size for adding a node") {
        Utils.estimateNodeSizeIncrement(Utils.toBytes("test"), ExpValue(("foobar", 2))) should equal(21)
      }
    }
  }
}
