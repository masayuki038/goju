package net.wrap_trap.goju

import java.util
import java.util.BitSet

import scala.math.floor
import scala.math.log
import scala.math.pow

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class Bloom(var e: Double, var n: Int) {

  var mb:Int = _
  var a = List.empty[BitSet]

  if (n <= 0)
    throw new IllegalArgumentException("n should be grater than 0.")

  if (e <= 0.0 || e > 1.0)
    throw new IllegalArgumentException("e should be 0.0 < e < 1.0")

  if (n >= 4 / e) {
    prepare(BloomMode.Size, n, e)
  } else {
    prepare(BloomMode.Bit, n, e)
  }

  def this(size: Int) {
    this(0.001, size)
  }
  def add(key: Constants.Key) {
    val hashes = makeHashes(key)
    hashAdd(hashes)
  }

  def member(key: Constants.Key): Boolean = {
    hashMember(makeHashes(key))
  }

  def log2(e: Double): Double = {
    log(e) / log(2)
  }

  def prepare(mode: BloomMode, n1: Int, e1: Double) = {
    this.e = e1
    this.n = n1

    val k = mode match {
      case BloomMode.Size => 1 + floor(log2(1 / this.e)).toInt
      case BloomMode.Bit => 1
      case _ => throw new IllegalArgumentException("BloomMode should be size or bit.")
    }

    val p = pow(this.e, 1 / k)

    this.mb = mode match {
      case BloomMode.Size => 1 + (-1 * floor(log2(1 - pow(1 - p, 1 / this.n)))).toInt
      case BloomMode.Bit => this.n
      case _ => throw new IllegalArgumentException("BloomMode should be size or bit.")
    }

    val m = 1 << this.mb
    this.n = (Math.floor(Math.log(1 - p) / Math.log(1 - 1 / m))).intValue()


    for (i <- 1 to k) {
      this.a = new BitSet :: this.a
    }
    //logger.info(String.format("mb: %d, n: %d, k: %d", this.mb, this.n, k));
  }

  def makeIndexes(mask: Int, hashes: Int): (Int, Int) = {
    (((hashes >> 16) & mask), (hashes & mask))
  }

  def hashAdd(hashes: Int) = {
    val mask = (1 << this.mb) - 1
    val (e1, e2) = makeIndexes(mask, hashes)
    setBits(mask, e1, e2)
  }

  def setBits(mask: Int, i1: Int, i2: Int) {
    //logger.debug(String.format("setBit mask: %d, i1: %d, i2: %d", mask, i1, i2));
    var i = i2
    for (bitmap <- this.a) {
      bitmap.set(i)
      //logger.debug("bitmap.set(i) i: " + i);
      i = (i + i1) & mask
    }
  }

  def hashMember(hashes: Int): Boolean = {
    val mask = (1 << this.mb) - 1
    val (e1, e2) = makeIndexes(mask, hashes)
    allSet(mask, e1, e2)
  }

  def allSet(mask: Int, i1: Int, i2: Int): Boolean = {
    //logger.debug(String.format("allSet mask: %d, i1: %d, i2: %d", mask, i1, i2));
    var i = i2
    for (bitmap <- this.a) {
      if (!bitmap.get(i))
        return false
      i = (i + i1) & mask
    }
    true
  }

  def makeHashes(key: Constants.Key): Int = {
    val hashCode = util.Arrays.hashCode(key)
    //logger.debug(String.format("hashCode: %d", hashCode));
    hashCode
  }

  object BloomMode {
    case object Size extends BloomMode
    case object Bit extends BloomMode
  }
  sealed abstract class BloomMode
}
