package net.wrap_trap.goju

import scala.language.implicitConversions
import scala.language.reflectiveCalls

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone
 *
 * Copyright (c) 2016 Masayuki Takahashi
 *
 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
object Helper {

  def using[A, R <: { def close() }](r: R)(f: R => A): A = {
    try {
      f(r)
    } finally {
      try { r.close() } catch { case _: Exception => }
    }
  }

  def using[A, R1 <: { def close() }, R2 <: { def close() }](r1: R1, r2: R2)(
      f: (R1, R2) => A): A = {
    try {
      f(r1, r2)
    } finally {
      try { r1.close() } catch { case _: Exception => }
      try { r2.close() } catch { case _: Exception => }
    }
  }

  def using[A, R1 <: { def close() }, R2 <: { def close() }, R3 <: { def close() }](
      r1: R1,
      r2: R2,
      r3: R3)(f: (R1, R2, R3) => A): A = {
    try {
      f(r1, r2, r3)
    } finally {
      try { r1.close() } catch { case _: Exception => }
      try { r2.close() } catch { case _: Exception => }
      try { r3.close() } catch { case _: Exception => }
    }
  }

  implicit def toBytes(key: Key): Array[Byte] = {
    key.bytes
  }
}
