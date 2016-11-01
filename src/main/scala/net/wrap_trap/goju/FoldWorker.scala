package net.wrap_trap.goju

import net.wrap_trap.goju.Constants.Value
import net.wrap_trap.goju.element.KeyValue

import scala.collection.mutable.Stack
import akka.actor.{Terminated, Actor, ActorRef}

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
      enter(pid, (key, Limit))
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (LevelResult, pid: String, key: Array[Byte], value: Value) => {
      enter(pid, (key, value))
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (PlainRpcProtocol.call, (LevelResults, pid: String, kv: KeyValue)) => {
      sendReply(sender(), Ok)
      enterMany(pid, kv)
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
        refQueues.find(e => {
          val (k, _) = e
          k == target
        }) match {
          case Some(queuePid) => {
            val (pid, queue) = queuePid
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
//    val (firstPid, firstKv) = this.folding.head
//    val rest = this.folding.tail
//
//    val lowest = rest.foldLeft((firstKv, List(firstPid)))((acc, e) => {
//      val (retKv, pidList) = acc
//      e match {
//        case (pid, kv: KeyValue) if kv.key < retKv.key
//      }
//    })
  }

  private def enter(pid: String, kv: Any) = {
  }

  private def enterMany(pid: String, kv: Any) = {
  }
}


