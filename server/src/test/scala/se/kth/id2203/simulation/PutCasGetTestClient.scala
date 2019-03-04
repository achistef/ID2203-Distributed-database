/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.simulation

import java.util.UUID
import se.kth.id2203.kvstore._
import se.kth.id2203.networking._
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.sl._
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.timer.Timer
import se.sics.kompics.sl.simulator.SimulationResult
import collection.mutable

class PutCasGetTestClient extends ComponentDefinition {

  //******* Ports ******
  val net: PositivePort[Network] = requires[Network]
  val timer: PositivePort[Timer] = requires[Timer]
  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  val server: NetAddress = cfg.getValue[NetAddress]("id2203.project.bootstrap-address")
  private val putPending: mutable.Map[UUID, String] = mutable.Map.empty
  private val casPending: mutable.Map[UUID, String] = mutable.Map.empty
  private val getPending: mutable.Map[UUID, String] = mutable.Map.empty
  //******* Handlers ******
  ctrl uponEvent {
    case _: Start => handle {
      val range = SimulationResult[String]("debugCode6")
      val boundaries = range.split('-')
      for (i <- boundaries(0).toInt to boundaries(1).toInt) {
        val put = Put(i.toString, i.toString, self)
        val putMsg = RouteMsg(put.key, put)
        trigger(NetMessage(self, server, putMsg) -> net)
        putPending += (put.id -> put.key)
      }
    }
  }

  net uponEvent {
    case NetMessage(_, OpResponse(id, _, value))if putPending contains id => handle {
      val resKey = putPending(id).toInt
      // successful for odd numbers
      val prevValue = if(resKey % 2 == 1) resKey else -resKey
      val cas = Cas(resKey.toString, prevValue.toString, (-resKey).toString, self)
      val casMsg = RouteMsg(cas.key, cas)
      trigger(NetMessage(self, server, casMsg) -> net)
      casPending += (cas.id -> cas.key)
    }
    case NetMessage(_, OpResponse(id, _, value))if casPending contains id => handle {
      val resKey = casPending(id)
      val get = Get(resKey.toString, self)
      val getMsg = RouteMsg(get.key, get)
      trigger(NetMessage(self, server, getMsg) -> net)
      getPending += (get.id -> get.key)
    }
    case NetMessage(_, OpResponse(id, _, value))if getPending contains id => handle {
      val resKey = getPending(id)
      SimulationResult += ("put/cas/get:" + resKey -> value.get)
    }
  }
}
