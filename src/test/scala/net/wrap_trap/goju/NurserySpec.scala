package net.wrap_trap.goju

import java.io.File

import akka.actor.{ActorSystem, ActorRef}
import akka.testkit.{TestKit, TestActorRef}
import net.wrap_trap.goju.element.KeyValue
import org.scalatest.{FlatSpecLike, BeforeAndAfter, Matchers, FlatSpec}

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class NurserySpec extends TestKit(ActorSystem("test"))
 with FlatSpecLike with Matchers with BeforeAndAfter {

  after {
    new File("./nursery.log").delete
  }

  "newNursery" should "return new Nursery" in {
    val nursery = Nursery.newNursery(".", 1, 2)
    nursery.isInstanceOf[Nursery] should be(true)
  }

  "newNursery" should "create log file" in {
    val nursery = Nursery.newNursery(".", 1, 2)
    new File("./nursery.log").exists should be(true)
  }

  def newNursery(testCode: (Nursery, ActorRef) => Any) {
    testCode(Nursery.newNursery(".", 1, 2), TestActorRef[PlainRpcActor])
  }

//  "Nursery.add" should "be added KeyValue" in newNursery { (nursery, top) =>
//    val key = Utils.toBytes("foo")
//    Nursery.add(key, "bar", 600, nursery, top)
//    nursery.tree.size should be(1)
//    nursery.tree.containsKey(key) should be(true)
//    nursery.tree.get(key).isInstanceOf[KeyValue] should be(true)
//    nursery.tree.get(key).asInstanceOf[KeyValue].value should be("bar")
//  }
}
