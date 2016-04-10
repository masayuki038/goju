package net.wrap_trap.goju.samples

import akka.actor.Actor

/**
  * Created by masayuki on 2016/03/04.
  */
class HelloAkka extends Actor {

  var internalState = ""

  def receive = {
    case "Hello" => {
      internalState = "Hello"
      println("World")
    }
    case "How are you?" => {
      internalState = "How are you?"
      sender ! "I'm fine. thank you"
    }
  }

  def state = internalState
}
