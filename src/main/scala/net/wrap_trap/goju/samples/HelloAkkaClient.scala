package net.wrap_trap.goju.samples

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by masayuki on 2016/03/04.
  */
object HelloAkkaClient {
  def main(args: Array[String]) = {
    val system = ActorSystem("system")
    val actor = system.actorOf(Props[HelloAkka])
    actor ! "Hello"

    implicit val timeout = Timeout(5 seconds)
    val reply = actor ? "How are you?"

    reply onSuccess {
      case msg: String => println("reply from actor: " + msg)
    }
  }
}
