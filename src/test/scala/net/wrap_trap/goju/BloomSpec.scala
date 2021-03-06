package net.wrap_trap.goju

import org.scalatest.{Matchers, FlatSpec}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class BloomSpec extends FlatSpec with Matchers {
  "Bloom" should "has the key" in {
    val bloom = new Bloom(10)
    bloom.add(Key("test".getBytes))
    bloom.member(Key("test".getBytes)) should equal(true)
  }

  "Bloom" should "not have the key" in {
    val bloom = new Bloom(10)
    bloom.add(Key("test".getBytes))
    bloom.member(Key("test2".getBytes)) should equal(false)
  }
}
