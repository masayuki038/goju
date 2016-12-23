package net.wrap_trap.goju

import akka.util.Timeout
import akka.event.Logging
import net.wrap_trap.goju.element.{Element, KeyValue}

import scala.collection.mutable.Stack
import akka.actor.{Terminated, Actor, ActorRef, Stash}

import scala.concurrent.duration._

/**
  * goju: HanoiDB(LSM-trees (Log-Structured Merge Trees) Indexed Storage) clone

  * Copyright (c) 2016 Masayuki Takahashi

  * This software is released under the MIT License.
  * http://opensource.org/licenses/mit-license.php
  */
class FoldWorker(val sendTo: ActorRef) extends Actor with PlainRpc with Stash {
  val log = Logging(context.system, this)

  var prefixFolders = List.empty[String]
  var folding = List.empty[(String, Option[KeyValue])]
  var refQueues = List.empty[(String, Stack[Any])]
  var pids = List.empty[String]
  var savePids = List.empty[String]

  val callTimeout = Settings.getSettings().getInt("goju.level.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  def receive = {
    case (Prefix, refList: List[String]) => {
      log.debug("receive Prefix, refList: %s".format(refList.foldLeft("")((acc, s) => acc + "," + s)))
      this.prefixFolders = refList
    }
    case (Initialize, refList: List[String]) => {
      log.debug("receive Initialize, refList: %s".format(refList.foldLeft("")((acc, s) => acc + "," + s)))
      this.savePids = this.prefixFolders ::: refList
      this.refQueues = for (pid <- this.savePids) yield (pid, new Stack[Any])
      this.folding = for (pid <- this.savePids) yield (pid, None: Option[KeyValue])
      fill()
    }
    case Terminated => {
      log.debug("receive Terminated")
      // do nothing
    }
    case _ => {
      stash()
    }
  }

  def receive2(): Receive = {
    case (LevelDone, pid: String) if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelDone, pid: %s".format(pid))
      enter(pid, Done)
      this.pids = this.pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (LevelLimit, pid: String, key: Key) if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelLimit, pid: %s, key: %s".format(pid, Utils.toHexStrings(key.bytes)))
      enter(pid, KeyValue(key, Limit, None))
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (LevelResult, pid: String, e: KeyValue) if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelResult, pid: %s, e.key: %s".format(pid, Utils.toHexStrings(e.key.bytes)))
      enter(pid, e)
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
    case (PlainRpcProtocol.call, (LevelResults, pid: String, kvs: List[KeyValue]))
      if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelResults, pid: %s")
      sendReply(sender(), Ok)
      enterMany(pid, kvs)
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
    }
  }

  private def fill(): Unit = {
    log.debug("fill, this.savePids: " + this.savePids.foldLeft("")((ret, s) => ret + "," + s))
    this.savePids.length match {
      case 0 => emitNext()
      case _ => {
        val target = this.savePids.head
        refQueues.find{ case(k, _) => k == target} match {
          case Some((pid, queue)) => {
            queue.isEmpty match {
              case true => {
                log.debug("fill, queue is empty")
                this.pids = List(pid)
                context.become(receive2)
                unstashAll()
              }
              case false => {
                log.debug("fill, queue is not empty")
                // Not calling keyreplace since queue is mutable.
                queue.pop() match {
                  case Done => {
                    log.debug("fill, queue.pop(): Done")
                    this.folding = this.folding.filter(p => p._1 != pid)
                    this.savePids = this.savePids.tail
                    fill()
                  }
                  case kv: KeyValue => {
                    log.debug("fill, queue.pop(): KeyValue")
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
    log.debug("emitNext, this.folding: " + this.folding.foldLeft("")((ret, s) => ret + "," + s))
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
        log.debug("emitNext, case (kv, fillForm) ig kv.tombstoned")
        this.savePids = fillForm
        fill()
      }
      case (KeyValue(k, Limit, _), _) => {
        log.debug("emitNext, case (KeyValue(k, Limit, _), _)")
        cast(this.sendTo, (FoldLimit, self, k))
      }
      case (kv, fillForm) => {
        log.debug("emitNext, case (kv, fillForm)")
        call(this.sendTo, (FoldResult, self, kv))
        this.savePids = fillForm
        fill()
      }
    }
  }

  private def enter(pid: String, message: Any): Unit = {
    log.debug("enter, pid: %s, message: %s".format(pid, message))
    this.refQueues.find{ case (p, _) => p == pid}
      .foreach{ case (_, queue) => queue.push(message)}
  }

  private def enterMany(pid: String, messages: List[Any]) = {
    log.debug("enterMany, pid: %s".format(pid))
    this.refQueues.find{ case (p, _) => p == pid}
      .foreach{ case (_, queue) => for (message <- messages) { queue.push(message) }}
  }
}


