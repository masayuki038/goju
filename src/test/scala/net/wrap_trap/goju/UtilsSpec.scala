package net.wrap_trap.goju

import net.wrap_trap.goju.element.KeyValue
import org.joda.time.DateTime
import org.scalatest.{FunSpec, BeforeAndAfter, Matchers, FlatSpec}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

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
}
