package net.wrap_trap.goju

import org.scalatest.{Matchers, FlatSpec}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class KeySpec  extends FlatSpec with Matchers {
  "==" should "be evaluted as equal" in {
    val a = Key(Array[Byte](0x01, 0x02, 0x03))
    val b = Key(Array[Byte](0x01, 0x02, 0x03))
    (a == b) should be (true)
  }

  "<" should "be evaluted as 'less than' lexicographically" in  {
    val a = Key(Array[Byte](0x01, 0x02, 0x00))
    val b = Key(Array[Byte](0x01, 0x03))
    (a < b) should be(true)

    val c = Key(Array[Byte](0x01, 0x02, 0x00))
    val d = Key(Array[Byte](0x01, 0x01))
    (c < d) should be(false)
  }

  "<=" should "be evaluted as 'less than or equal'" in  {
    val a = Key(Array[Byte](0x01, 0x02, 0x03))
    val b = Key(Array[Byte](0x01, 0x02, 0x03))
    (a <= b) should be(true)

    val c = Key(Array[Byte](0x01, 0x02, 0x00))
    val d = Key(Array[Byte](0x01, 0x03))
    (c <= d) should be(true)
  }

  ">" should "be evaluted as 'more than' lexicographically" in  {
    val a = Key(Array[Byte](0x01, 0x03))
    val b = Key(Array[Byte](0x01, 0x02, 0x03))
    (a > b) should be(true)

    val c = Key(Array[Byte](0x01, 0x01))
    val d = Key(Array[Byte](0x01, 0x02, 0x03))
    (c > d) should be(false)
  }

  ">=" should "be evaluted as 'more than' or equal" in  {
    val a = Key(Array[Byte](0x01, 0x02, 0x03))
    val b = Key(Array[Byte](0x01, 0x02, 0x03))
    (a >= b) should be(true)

    val c = Key(Array[Byte](0x01, 0x03))
    val d = Key(Array[Byte](0x01, 0x02, 0x03))
    (c >= d) should be(true)
  }
}
