package net.wrap_trap.goju

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
  var prefixFolders: Option[List[String]] = None
  var values: Option[List[(String, Option[Any])]] = None
  var queues: Option[List[(String, Stack[Any])]] = None
  var pids: Option[List[String]] = None
  var savePids : Option[List[String]] = None

  def receive = {
    case (Prefix, refList: List[String]) => {
      this.prefixFolders = Option(refList)
    }
    case (Initialize, refList: List[String]) => {
      val folders = this.prefixFolders.get ::: refList
      val queues = for(pid <- folders) yield (pid, new Stack[Any])
      val initial = for(pid <- folders) yield (pid, None)
      fill(initial.toList, queues.toList, folders.toList)
    }
    case Terminated => // do nothing
  }

  private def fill(vs: List[(String, Option[Any])],
                   qs: List[(String, Stack[Any])],
                   folders: List[String]): Unit = {
    folders.length match {
      case 0 => emitNext(vs, qs)
      case _ => {
        val target = folders.head
        qs.find(e => {
          val (k, _) = e
          k == target
        }) match {
          case Some(queuePid) => {
            val (pid, queue) = queuePid
            queue.isEmpty match {
              case true => {
                this.values = Option(vs)
                this.queues = Option(qs)
                this.pids =  Option(List(pid))
                this.savePids = Option(folders)
              }
              case false => {
                // Not calling keyreplace since queue is mutable.
                queue.pop() match {
                  case Done => {
                    val deleted = vs.filter(p => p._1 != pid)
                    fill(deleted, qs, folders.tail)
                  }
                  case kv: KeyValue => {
                    val vs2 = vs.map(p => {
                      val (initialPid, _) = p
                      if(initialPid == pid) {
                        (initialPid, Option(kv))
                      } else {
                        p
                      }
                    })
                    fill(vs2, qs, folders.tail)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private def emitNext(vs: List[(String, Option[Any])], qs: List[(String, Stack[Any])]) = {}
}


