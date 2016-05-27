package com.test

import msgpack4z._
import org.scalatest.{Matchers, FlatSpec}

/**
  * Created by masayuki on 2016/05/25.
  */
class Msgpack4zCoreSpec extends FlatSpec with Matchers {
  "List[Int]" should "serialize and deserialize" in {
    import msgpack4z.CodecInstances.all._
    val target = {
      List(new Binary(Array(0x00.asInstanceOf[Byte], 0x01.asInstanceOf[Byte])))
    }
    val packed = MsgpackCodec[List[Binary]].toBytes(target, MsgOutBuffer.create())
    val unpacked = MsgpackCodec[List[Binary]].unpackAndClose(MsgInBuffer(packed))
    unpacked.getOrElse(List.empty[Array[Byte]]) should be(target)
  }
}
