package net.wrap_trap.goju

import akka.util.Timeout
import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.{Element, KeyValue}

import scala.collection.mutable.Stack
import akka.actor.{Terminated, Actor, ActorRef}

import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class FoldWorker(val sendTo: ActorRef) extends Actor with PlainRpc {
  var prefixFolders = List.empty[String]
  var folding = List.empty[(String, Option[KeyValue])]
  var refQueues = List.empty[(String, Stack[Any])]
  var pids = List.empty[String]
  var savePids = List.empty[String]

  val callTimeout = Settings.getSettings().getInt("goju.level.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  def receive = {
    case (Prefix, refList: List[String]) => {
      this.prefixFolders = refList
    }
    case (Initialize, refList: List[String]) => {
      this.savePids = this.prefixFolders ::: refList
      this.refQueues = for(pid <- this.savePids) yield (pid, new Stack[Any])
      this.folding = for(pid <- this.savePids) yield (pid, None: Option[KeyValue])
      fill()
    }
    case Terminated => // do nothing
    case (LevelDone, pid: String) => {
      enter(pid, Done)
      this.pids = this.pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (LevelLimit, pid: String, key: Array[Byte]) => {
      enter(pid, new KeyValue(key, Limit))
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (LevelResult, pid: String, key: Array[Byte], value: Value) => {
      enter(pid, new KeyValue(key, value))
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (PlainRpcProtocol.call, (LevelResults, pid: String, kvs: List[KeyValue])) => {
      sendReply(sender(), Ok)
      enterMany(pid, kvs)
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
  }

  private def fill(): Unit = {
    savePids.length match {
      case 0 => emitNext()
      case _ => {
        val target = savePids.head
        refQueues.find{ case(k, _) => k == target} match {
          case Some((pid, queue)) => {
            queue.isEmpty match {
              case true => {
                this.pids = List(pid)
              }
              case false => {
                // Not calling keyreplace since queue is mutable.
                queue.pop() match {
                  case Done => {
                    this.folding = this.folding.filter(p => p._1 != pid)
                    this.savePids = this.savePids.tail
                    fill()
                  }
                  case kv: KeyValue => {
                    this.folding  = this.folding .map(p => {
                      val (initialPid, _) = p
                      if(initialPid == pid) {
                        (initialPid, Option(kv))
                      } else {
                        p
                      }
                    })
                    this.savePids = this.savePids.tail
                    fill()
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private def emitNext(): Unit = {
    if(this.folding.isEmpty) {
      cast(this.sendTo, (FoldDone, self))
      return
    }

    val (firstPid, maybeFirstKv) = this.folding.head
    val rest = this.folding.tail

    val firstKv = maybeFirstKv match {
      case Some(kv) => kv
      case _ => throw new IllegalStateException("firstKv is None")
    }

    val lowest = rest.foldLeft((firstKv, List(firstPid)))((acc, e) => {
      val (retKv, pidList) = acc
      e match {
        case (pid, Some(kv: KeyValue)) if kv.key < retKv.key => {
          (kv, List(pid))
        }
        case (pid, Some(element: Element)) => {
          (element, pid :: pidList)
        }
        case (_, _) => acc
      }
    })

    lowest match {
      case (kv, fillForm) if kv.tombstoned => {
        this.savePids = fillForm
        fill()
      }
      case (KeyValue(k, Limit, _), _) => cast(this.sendTo, (FoldLimit, self, k))
      case (kv, fillForm) => {
        call(this.sendTo, (FoldResult, self, kv))
        fill()
      }
    }
  }

  private def enter(pid: String, message: Any): Unit = {
    this.refQueues.find{ case (p, _) => p == pid}
      .foreach{ case (_, queue) => queue.push(message)}
  }

  private def enterMany(pid: String, messages: List[Any]) = {
    this.refQueues.find{ case (p, _) => p == pid}
      .foreach{ case (_, queue) => for (message <- messages) { queue.push(message) }}
  }
}


