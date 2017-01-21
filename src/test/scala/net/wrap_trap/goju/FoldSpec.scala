package net.wrap_trap.goju

import java.io.{File, FileWriter, RandomAccessFile}
import java.nio.file.Files

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.TestKit
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.Helper._
import org.scalatest._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class FoldSpec extends FlatSpecLike
  with ShouldMatchers
  with BeforeAndAfter
  with PlainRpcClient {

  val log = Logging(Utils.getActorSystem, this)

  "To fold 1024 entries" should "return all entries in levels" in {
    TestHelper.deleteDirectory(new File("test-fold1"))
    val goju = Goju.open("test-fold1")
    (1 to 1024).foreach(i => goju.put(Utils.toBytes("key" + i), "value" + i))
    Thread.sleep(5000L)

    var ret = goju.foldRange((k, v, acc) => {
      val (count, list) = acc
      (count + 1, v :: list)
    }, (0, List.empty[Value]),
      KeyRange(Key(Utils.toBytes("key1")), true, Option(Key(Utils.toBytes("key3"))), false, Integer.MAX_VALUE))
    val set12 = ret.toSet
    log.debug("content set12: " + ret.mkString(", "))
    (1 to 2).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})
    (10 to 29).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})
    (100 to 299).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})
    (1000 to 1024).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})

    ret = goju.foldRange((k, v, acc) => {
      val (count, list) = acc
      (count + 1, v :: list)
    }, (0, List.empty[Value]),
      KeyRange(Key(Utils.toBytes("key3")), true, Option(Key(Utils.toBytes("key5"))), false, Integer.MAX_VALUE))
    val set34 = ret.toSet
    log.debug("content set34: " + ret.mkString(", "))
    (3 to 4).foreach(i => withClue("value" + i){set34("value" + i) should be(true)})
    (30 to 49).foreach(i => withClue("value" + i){set34("value" + i) should be(true)})
    (300 to 499).foreach(i => withClue("value" + i){set34("value" + i) should be(true)})

    ret= goju.foldRange((k, v, acc) => {
      val (count, list) = acc
      (count + 1, v :: list)
    }, (0, List.empty[Value]),
      KeyRange(Key(Utils.toBytes("key5")), true, Option(Key(Utils.toBytes("key7"))), false, Integer.MAX_VALUE))
    val set56 = ret.toSet
    log.debug("content set56: " + ret.mkString(", "))
    (5 to 6).foreach(i => withClue("value" + i){set56("value" + i) should be(true)})
    (50 to 69).foreach(i => withClue("value" + i){set56("value" + i) should be(true)})
    (500 to 699).foreach(i => withClue("value" + i){set56("value" + i) should be(true)})

    ret = goju.foldRange((k, v, acc) => {
      val (count, list) = acc
      (count + 1, v :: list)
    }, (0, List.empty[Value]),
      KeyRange(Key(Utils.toBytes("key7")), true, Option(Key(Utils.toBytes("key9"))), false, Integer.MAX_VALUE))
    val set78 = ret.toSet
    log.debug("content set78: " + ret.mkString(", "))
    (7 to 8).foreach(i => withClue("value" + i){set78("value" + i) should be(true)})
    (70 to 89).foreach(i => withClue("value" + i){set78("value" + i) should be(true)})
    (700 to 899).foreach(i => withClue("value" + i){set78("value" + i) should be(true)})

    ret = goju.foldRange((k, v, acc) => {
      val (count, list) = acc
      (count + 1, v :: list)
    }, (0, List.empty[Value]),
      KeyRange(Key(Utils.toBytes("key9")), true, None, false, Integer.MAX_VALUE))
    val set9 = ret.toSet
    log.debug("content set9:" + ret.mkString(", "))
    set9.contains("value9") should be(true)
    (90 to 99).foreach(i => withClue("value" + i){set9("value" + i) should be(true)})
    (900 to 999).foreach(i => withClue("value" + i){set9("value" + i) should be(true)})
    Thread.sleep(10000L)
    goju.destroy()
  }

  "To fold entries" should "return all entries without tombstoned in levels" in {
    TestHelper.deleteDirectory(new File("test-fold1"))
    val goju = Goju.open("test-fold1")
    (1 to 1024).foreach(i => goju.put(Utils.toBytes("key" + i), "value" + i))
    Thread.sleep(5000L)
    goju.delete(Utils.toBytes("key1"))
    goju.delete(Utils.toBytes("key150"))
    goju.delete(Utils.toBytes("key1024"))

    val ret = goju.foldRange((k, v, acc) => {
      val (count, list) = acc
      (count + 1, v :: list)
    }, (0, List.empty[Value]),
      KeyRange(Key(Utils.toBytes("key1")), true, Option(Key(Utils.toBytes("key2"))), false, Integer.MAX_VALUE))
    val set12 = ret.toSet
    log.debug("content set12: " + ret.mkString(", "))
    goju.get(Utils.toBytes("key1")) shouldNot be(defined)
    goju.get(Utils.toBytes("key150")) shouldNot be(defined)
    goju.get(Utils.toBytes("key1024")) shouldNot be(defined)

    (10 to 19).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})
    (100 to 149).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})
    (151 to 199).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})
    (1000 to 1023).foreach(i => withClue("value" + i){set12("value" + i) should be(true)})
  }
}
