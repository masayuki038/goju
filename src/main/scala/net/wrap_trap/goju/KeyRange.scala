package net.wrap_trap.goju

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
case class KeyRange(
    fromKey: Key,
    fromInclude: Boolean,
    toKey: Option[Key],
    toInclude: Boolean,
    limit: Int) {

  def keyInFromRange(thatKey: Key): Boolean = {
    this.fromKey.bytes.length match {
      case 0 => true
      case _ =>
        if (fromInclude) {
          fromKey <= thatKey
        } else {
          fromKey < thatKey
        }
    }
  }

  def keyInToRange(thatKey: Key): Boolean = {
    this.toKey match {
      case Some(to) =>
        if (toInclude) {
          to >= thatKey
        } else {
          to > thatKey
        }
      case None => true
    }
  }

  override def toString: String = {
    val hFromKey = Utils.toHexStrings(this.fromKey.bytes)
    val sFromKey = try { Utils.fromBytes(this.fromKey.bytes) } catch { case _: Exception => "-" }
    val (hToKey, sToKey) = this.toKey match {
      case Some(t) =>
        val hToKey = Utils.toHexStrings(t.bytes)
        val sToKey = try { Utils.fromBytes(t.bytes) } catch { case _: Exception => "-" }
        (hToKey, sToKey)
      case None => ("-", "-")
    }

    "keyRange, fromKey: %s, fromKey(string): %s, fromInclude: %s, toKey: %s, toKey(string): %s, toInclude: %s"
      .format(hFromKey, sFromKey, this.fromInclude, hToKey, sToKey, this.toInclude)
  }
}
