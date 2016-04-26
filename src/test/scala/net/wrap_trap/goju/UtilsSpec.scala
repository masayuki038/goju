package net.wrap_trap.goju

import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class UtilsSpec  extends FlatSpec with Matchers with BeforeAndAfter {

  "now + 1min" should "not be expired" in {
    Utils.hasExpired(DateTime.now.plusMinutes(1)) should equal(false)
  }

  "now - 1min" should "be expired" in {
    Utils.hasExpired(DateTime.now.plusMinutes(-1)) should equal(true)
  }
}
