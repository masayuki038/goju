package net.wrap_trap.goju

import scala.io.Source

import net.wrap_trap.goju.Constants._

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class Writer {

}

case class Node(level: Int, members: Array[(Key, ExpValue)], size: Int = 0)
case class State(indexFile: Source,
                  indexFilePos: Int,
                  lastNodePos: Long,
                  lastNodeSize: Int,
                  nodes: Array[Node],
                  name: String,
                  bloom: Bloom,
                  opts: Array[Any],
                  valueCount: Int = 0,
                  tombstoneCount: Int = 0)
