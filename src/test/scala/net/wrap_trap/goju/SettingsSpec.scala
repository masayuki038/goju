package net.wrap_trap.goju

import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class SettingsSpec extends FlatSpec with Matchers with BeforeAndAfter {
  var settings: Settings = _
  before {
    settings = Settings.getSettings
  }

  "Settings" should "return a string value" in {
    settings.getString("test.foo", "hoge") should equal("bar")
  }

  "Settings" should "return a default string value when the key is not" in {
    settings.getString("test.fo", "hoge") should equal("hoge")
  }

  "Settings" should "return a int value" in {
    settings.getInt("test.some_int", 0) should equal(1)
  }

  "Settings" should "return a default int value when the key is not" in {
    settings.getInt("test.some_in", 0) should equal(0)
  }
}
