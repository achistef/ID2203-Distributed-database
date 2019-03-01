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

class CrashTestClient extends ComponentDefinition {

  //******* Ports ******
  val net: PositivePort[Network] = requires[Network]
  val timer: PositivePort[Timer] = requires[Timer]
  //******* Fields ******
  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address")
  val server: NetAddress = cfg.getValue[NetAddress]("id2203.project.bootstrap-address")
  private val pending: mutable.Map[UUID, String] = mutable.Map.empty
  //******* Handlers ******
  ctrl uponEvent {
    case _: Start => handle {
      val range = SimulationResult[String]("debugCode7")
      val boundaries = range.split('-')
      for (value <- boundaries(0).toInt to boundaries(1).toInt){
        val put = Put(value.toString, value.toString, self)
        val putMsg = RouteMsg(put.key, put)
        trigger(NetMessage(self, server, putMsg) -> net)

        //KILLS ONLY ONE PROCESS PER PARTITION.
        val killOp = Debug("Kill/key:"+value, self)
        val routeMsg = RouteMsg(killOp.key, killOp)
        trigger(NetMessage(self, server, routeMsg) -> net)


        val get = Get(value.toString, self)
        val getMsg = RouteMsg(get.key, get)
        trigger(NetMessage(self, server, getMsg) -> net)
        pending += (get.id -> get.key)

      }
    }
  }

  net uponEvent {
    case NetMessage(_, OpResponse(id, _, value))if pending contains id => handle {
      val resKey = pending(id)
      SimulationResult += ("put/get:" + resKey -> value.get)
    }
  }
}
