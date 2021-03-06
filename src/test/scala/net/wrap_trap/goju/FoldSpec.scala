package net.wrap_trap.goju

import java.io.File
import java.util.concurrent.TimeUnit

import net.wrap_trap.goju.Constants.Value
import org.scalatest._
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

 * Copyright (c) 2016 Masayuki Takahashi

 * This software is released under the MIT License.
 * http://opensource.org/licenses/mit-license.php
 */
class FoldSpec extends FlatSpecLike with ShouldMatchers with BeforeAndAfter with PlainRpcClient {
  private val log = LoggerFactory.getLogger(this.getClass)

  "foldRange" should "return all entries in each level" in {
    TestHelper.deleteDirectory(new File("test-data/test-fold1"))
    val goju = Goju.open("test-data/test-fold1")
    (1 to 1024).foreach(i => goju.put(Utils.toBytes("key" + i), "value" + i))
    Thread.sleep(5000L)

    var ret = goju.foldRange(
      (_, v, acc) => {
        val (count, list) = acc
        (count + 1, v :: list)
      },
      (0, List.empty[Value]),
      KeyRange(
        Key(Utils.toBytes("key1")),
        fromInclude = true,
        Option(Key(Utils.toBytes("key3"))),
        toInclude = false,
        Integer.MAX_VALUE)
    )
    val set12 = ret.toSet
    log.debug("content set12: " + ret.mkString(", "))
    (1 to 2).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })
    (10 to 29).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })
    (100 to 299).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })
    (1000 to 1024).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })

    ret = goju.foldRange(
      (_, v, acc) => {
        val (count, list) = acc
        (count + 1, v :: list)
      },
      (0, List.empty[Value]),
      KeyRange(
        Key(Utils.toBytes("key3")),
        fromInclude = true,
        Option(Key(Utils.toBytes("key5"))),
        toInclude = false,
        Integer.MAX_VALUE)
    )
    val set34 = ret.toSet
    log.debug("content set34: " + ret.mkString(", "))
    (3 to 4).foreach(i => withClue("value" + i) { set34("value" + i) should be(true) })
    (30 to 49).foreach(i => withClue("value" + i) { set34("value" + i) should be(true) })
    (300 to 499).foreach(i => withClue("value" + i) { set34("value" + i) should be(true) })

    ret = goju.foldRange(
      (_, v, acc) => {
        val (count, list) = acc
        (count + 1, v :: list)
      },
      (0, List.empty[Value]),
      KeyRange(
        Key(Utils.toBytes("key5")),
        fromInclude = true,
        Option(Key(Utils.toBytes("key7"))),
        toInclude = false,
        Integer.MAX_VALUE)
    )
    val set56 = ret.toSet
    log.debug("content set56: " + ret.mkString(", "))
    (5 to 6).foreach(i => withClue("value" + i) { set56("value" + i) should be(true) })
    (50 to 69).foreach(i => withClue("value" + i) { set56("value" + i) should be(true) })
    (500 to 699).foreach(i => withClue("value" + i) { set56("value" + i) should be(true) })

    ret = goju.foldRange(
      (_, v, acc) => {
        val (count, list) = acc
        (count + 1, v :: list)
      },
      (0, List.empty[Value]),
      KeyRange(
        Key(Utils.toBytes("key7")),
        fromInclude = true,
        Option(Key(Utils.toBytes("key9"))),
        toInclude = false,
        Integer.MAX_VALUE)
    )
    val set78 = ret.toSet
    log.debug("content set78: " + ret.mkString(", "))
    (7 to 8).foreach(i => withClue("value" + i) { set78("value" + i) should be(true) })
    (70 to 89).foreach(i => withClue("value" + i) { set78("value" + i) should be(true) })
    (700 to 899).foreach(i => withClue("value" + i) { set78("value" + i) should be(true) })

    ret = goju.foldRange(
      (_, v, acc) => {
        val (count, list) = acc
        (count + 1, v :: list)
      },
      (0, List.empty[Value]),
      KeyRange(
        Key(Utils.toBytes("key9")),
        fromInclude = true,
        None,
        toInclude = false,
        Integer.MAX_VALUE)
    )
    val set9 = ret.toSet
    log.debug("content set9:" + ret.mkString(", "))
    set9.contains("value9") should be(true)
    (90 to 99).foreach(i => withClue("value" + i) { set9("value" + i) should be(true) })
    (900 to 999).foreach(i => withClue("value" + i) { set9("value" + i) should be(true) })
    goju.close()
    goju.terminate()
  }

  "foldRange" should "return all entries without tombstoned in each level" in {
    TestHelper.deleteDirectory(new File("test-data/test-fold1"))
    val goju = Goju.open("test-data/test-fold1")

    (1 to 1024).foreach(i => goju.put(Utils.toBytes("key" + i), "value" + i))
    Thread.sleep(5000L)
    goju.delete(Utils.toBytes("key1"))
    goju.delete(Utils.toBytes("key150"))
    goju.delete(Utils.toBytes("key1024"))

    val ret = goju.foldRange(
      (_, v, acc) => {
        val (count, list) = acc
        (count + 1, v :: list)
      },
      (0, List.empty[Value]),
      KeyRange(
        Key(Utils.toBytes("key1")),
        fromInclude = true,
        Option(Key(Utils.toBytes("key2"))),
        toInclude = false,
        Integer.MAX_VALUE)
    )
    val set12 = ret.toSet
    log.debug("content set12: " + ret.mkString(", "))
    set12(Utils.toBytes("key1")) should be(false)
    set12(Utils.toBytes("key150")) should be(false)
    set12(Utils.toBytes("key1024")) should be(false)

    (10 to 19).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })
    (100 to 149).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })
    (151 to 199).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })
    (1000 to 1023).foreach(i => withClue("value" + i) { set12("value" + i) should be(true) })
    goju.close()
    goju.terminate()
  }

  "foldRange" should "return limited entries" in {
    TestHelper.deleteDirectory(new File("test-data/test-fold1"))
    val goju = Goju.open("test-data/test-fold1")

    (1 to 1024).foreach(i => goju.put(Utils.toBytes("key" + i), "value" + i))
    Thread.sleep(5000L)

    val ret = goju.foldRange(
      (_, v, acc) => {
        val (count, list) = acc
        (count + 1, v :: list)
      },
      (0, List.empty[Value]),
      KeyRange(
        Key(Utils.toBytes("key1")),
        fromInclude = true,
        Option(Key(Utils.toBytes("key2"))),
        toInclude = false,
        9)
    )
    val set12 = ret.toSet
    log.debug("content set12: " + ret.mkString(", "))
    set12("value1") should be(true)
    set12("value10") should be(true)
    set12("value100") should be(true)
    set12("value1000") should be(true)
    set12("value1001") should be(true)
    set12("value1002") should be(true)
    set12("value1003") should be(true)
    set12("value1004") should be(true)
    set12("value1005") should be(true)
    set12("value1006") should be(false)
    set12.size should be(9)
    goju.close()
    goju.terminate()
  }
}
