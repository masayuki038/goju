package com.test

import org.scalatest._

import scala.collection.mutable

/**
  * Created by masayuki on 2016/03/02.
  */
class SetSpeck extends FlatSpec with Matchers {
  info("Starting...")

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new mutable.Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should equal(2)
    stack.pop() should equal(1)
    info("ok")
  }
}
