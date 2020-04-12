package net.wrap_trap.goju

import org.scalatest.Matchers
import org.scalatest.FlatSpec

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
class CompressSpec extends FlatSpec with Matchers {
  "compress" should "0x00 + plain" in {
    val body = Array[Byte](0x01, 0x02, 0x03)
    val compressor = Compress(0)
    val compressed = compressor.compress(body)
    compressed(0) should be(0x00)
    compressed should be(Array(0x00, 0x01, 0x02, 0x03))
  }

  "compress" should "throw IllegalArgumentException" in {
    val body = Array[Byte](0x01, 0x02, 0x03)
    val compressor = Compress(1)
    intercept[IllegalArgumentException] {
      compressor.compress(body)
    }
  }

  "decompress" should "return body" in {
    val compressed = Array[Byte](0x00, 0x01, 0x02, 0x03)
    val compressor = Compress(0)
    compressor.decompress(compressed) should be(Array(0x01, 0x02, 0x03))
  }

  "decompress" should "throw IllegalArgumentException" in {
    val compressed = Array[Byte](0x01, 0x01, 0x02, 0x03)
    val compressor = Compress(0)
    intercept[IllegalArgumentException] {
      compressor.decompress(compressed) should be(Array(0x01, 0x02, 0x03))
    }
  }
}
