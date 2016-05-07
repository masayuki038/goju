package net.wrap_trap.goju

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
object PlainRpcProtocol {
  final val call = 'CALL
  final val reply = 'REPLY
  final val cast = 'CAST

  def CALL(msg: Any): Tuple2[Symbol, Any] = {
    (call, msg)
  }

  def REPLY(msg: Any): Tuple2[Symbol, Any] = {
    (reply, msg)
  }

  def CAST(msg: Any): Tuple2[Symbol, Any] = {
    (cast, msg)
  }
}
