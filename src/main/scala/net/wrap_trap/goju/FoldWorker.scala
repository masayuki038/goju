package net.wrap_trap.goju

import akka.util.Timeout
import akka.event.Logging
import net.wrap_trap.goju.element.KeyValue

import scala.collection.mutable.Queue
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

  var prefixFolders = List.empty[ActorRef]
  var folding = List.empty[(ActorRef, Option[KeyValue])]
  var refQueues = List.empty[(ActorRef, Queue[Any])]
  var pids = List.empty[ActorRef]
  var savePids = List.empty[ActorRef]

  val callTimeout = Settings.getSettings().getInt("goju.level.call_timeout", 300)
  implicit val timeout = Timeout(callTimeout seconds)

  def receive = {
    case (Prefix, refList: List[ActorRef]) => {
      log.debug("receive Prefix, refList: %s".format(refList))
      this.prefixFolders = refList
    }
    case (Initialize, refList: List[ActorRef]) => {
      log.debug("receive Initialize, refList: %s".format(refList))
      this.savePids = this.prefixFolders ::: refList
      this.refQueues = for (pid <- this.savePids) yield (pid, new Queue[Any])
      this.folding = for (pid <- this.savePids) yield (pid, None: Option[KeyValue])
      fill()
    }
    case Terminated => {
      log.debug("receive Terminated")
      // do nothing
    }
    case _ => stash()
  }

  def receive2(): Receive = {
    case (LevelDone, pid: ActorRef) if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelDone, pid: %s, this.pids: %s".format(pid, this.pids))
      enter(pid, Done)
      this.pids = this.pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
      unstashAll()
    }
    case (LevelDone, pid: ActorRef) => {
      log.debug("receive LevelDone(stash), pid: %s, this.pids: %s".format(pid, this.pids))
      stash()
    }
    case (LevelLimit, pid: ActorRef, key: Key) if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelLimit, pid: %s, key: %s, this.pids: %s".format(pid, Utils.toHexStrings(key.bytes), this.pids))
      enter(pid, KeyValue(key, Limit, None))
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
      unstashAll()
    }
    case (LevelLimit, pid: ActorRef, key: Key) => {
      log.debug("receive LevelLimit(stash), pid: %s, key: %s, this.pids: %s".format(pid, Utils.toHexStrings(key.bytes), this.pids))
      stash()
    }
    case (LevelResult, pid: ActorRef, e: KeyValue) if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelResult, pid: %s, e.key: %s, this.pids: %s".format(pid, Utils.toHexStrings(e.key.bytes), this.pids))
      enter(pid, e)
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
      unstashAll()
    }
    case (LevelResult, pid: ActorRef, e: KeyValue) => {
      log.debug("receive LevelResult(stash), pid: %s, e.key: %s, this.pids: %s".format(pid, Utils.toHexStrings(e.key.bytes), this.pids))
      stash()
    }
    case (PlainRpcProtocol.call, (LevelResults, pid: ActorRef, kvs: List[KeyValue]))
      if((this.pids.size > 0) && (pid == this.pids.head)) => {
      log.debug("receive LevelResults, pid: %s, this.pids: %s".format(pid, this.pids))
      sendReply(sender(), Ok)
      enterMany(pid, kvs)
      this.pids = pids.filter(p => p != pid)
      if(this.pids.length == 0) {
        fill()
      }
      unstashAll()
    }
    case (PlainRpcProtocol.call, (LevelResults, pid: ActorRef, kvs: List[KeyValue])) => {
      log.debug("receive LevelResults(stash), pid: %s, this.pids: %s".format(pid, this.pids))
      stash()
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
                queue.dequeue() match {
                  case Done => {
                    log.debug("fill, queue.dequeue(): Done")
                    this.folding = this.folding.filter(p => p._1 != pid)
                    this.savePids = this.savePids.tail
                    fill()
                  }
                  case kv: KeyValue => {
                    log.debug("fill, queue.dequeue(): KeyValue")
                    this.folding  = this.folding.map(p => {
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
        case (pid, Some(kv: KeyValue)) if kv.key == retKv.key => {
          (retKv, pid :: pidList)
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

  private def enter(pid: ActorRef, message: Any): Unit = {
    log.debug("enter, pid: %s, message: %s".format(pid, message))
    this.refQueues.find{ case (p, _) => p == pid}
      .foreach{ case (_, queue) => queue.enqueue(message)}
  }

  private def enterMany(pid: ActorRef, messages: List[Any]) = {
    log.debug("enterMany, pid: %s".format(pid))
    this.refQueues.find{ case (p, _) => p == pid}
      .foreach{ case (_, queue) => for (message <- messages) { queue.enqueue(message) }}
  }
}


