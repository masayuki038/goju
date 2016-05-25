package com.test

import msgpack4z._
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by masayuki on 2016/05/25.
  */
class Msgpack4zCoreSpec extends FlatSpec with Matchers {
  "List[Int]" should "serialize and deserialize" in {
    import msgpack4z.CodecInstances.all._
    val target = List(1,2,3)
    val packed = MsgpackCodec[List[Int]].toBytes(target, MsgOutBuffer.create())
    val unpacked = MsgpackCodec[List[Int]].unpackAndClose(MsgInBuffer(packed))
    unpacked.getOrElse(List.empty[Int]) should be(target)
  }
}
