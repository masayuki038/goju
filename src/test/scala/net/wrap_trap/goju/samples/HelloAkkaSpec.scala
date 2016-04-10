package net.wrap_trap.goju.samples

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import net.wrap_trap.goju.StopSystemAfterAll
import org.scalatest.{MustMatchers, WordSpecLike}

/**
  * goju-to: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class HelloAkkaSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike
  with MustMatchers
  with StopSystemAfterAll {

  "HelloAkka" must {
    "reply 'World'" in {
      val actor = TestActorRef[HelloAkka]

      actor ! "Hello"
      actor.underlyingActor.state must (equal("Hello"))
    }
  }
}
