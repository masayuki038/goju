package net.wrap_trap.goju

import org.scalatest._

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class PlainRpcProtocolSpec extends FlatSpec with Matchers {

  "PlainRpc.CALL" should "replace to ('CALL, msg)" in {
    PlainRpcProtocol.CALL("some message") should equal('CALL, "some message")
  }

  "PlainRpc.REPLY" should "replace to ('REPLY, msg)" in {
    PlainRpcProtocol.REPLY("some reply") should equal('REPLY, "some reply")
  }

  "PlainRpc.CAST" should "replace to ('CAST, msg)" in {
    PlainRpcProtocol.CAST("some message") should equal('CAST, "some message")
  }
}
