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
package se.kth.id2203.kvstore;

import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.sics.kompics.sl._
import se.sics.kompics.network.Network

import scala.collection.mutable;

class KVService extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val route = requires(Routing);
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");

  // members
  val store = mutable.Map.empty[String,String]
  for(i <- 0 to 10){
    store += ((i.toString,(10-i).toString))
  }


  //******* Handlers ******
  net uponEvent {
    // TODO this is shown in server side
//    case NetMessage(header, op: Op ) => handle {
//      log.info("Got op operation {}!", op);
//      trigger(NetMessage(self, header.src, op.response(OpCode.Ok, None)) -> net);
//    }
    case NetMessage(header, get: Get ) => handle {
      log.info("Got GET operation {}!", get);
      val result = if(store contains get.key) Some(store(get.key)) else None
      trigger(NetMessage(self, header.src, get.response(OpCode.Ok, result)) -> net);
    }
    case NetMessage(header, put: Put ) => handle {
      log.info("Got PUT operation {}!", put);
      trigger(NetMessage(self, header.src, put.response(OpCode.Ok, None)) -> net);
    }
    case NetMessage(header, cas: Cas ) => handle {
      log.info("Got CAS operation {}!", cas);
      trigger(NetMessage(self, header.src, cas.response(OpCode.Ok, None)) -> net);
    }

  }
}
