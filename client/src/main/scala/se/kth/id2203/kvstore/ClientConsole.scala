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
package se.kth.id2203.kvstore

import com.larskroll.common.repl._
import com.typesafe.scalalogging.StrictLogging
import org.apache.log4j.Layout
import util.log4j.ColoredPatternLayout
import fastparse.all._
import se.kth.id2203.networking.NetAddress

import concurrent.{Await, Future}
import concurrent.duration._

object ClientConsole {
  // Better build this statically. Has some overhead (building a lookup table).
  val simpleStr = P(CharsWhileIn(('0' to '9') ++ ('a' to 'z') ++ ('A' to 'Z'), 1).!);
  val colouredLayout = new ColoredPatternLayout("%d{[HH:mm:ss,SSS]} %-5p {%c{1}} %m%n");
}

class ClientConsole(val service: ClientService) extends CommandConsole with ParsedCommands with StrictLogging {
  import ClientConsole._;

  override def layout: Layout = colouredLayout;
  override def onInterrupt(): Unit = exit();

  val getCommand = parsed(P("get" ~ " " ~ simpleStr), usage = "get <key>", descr = "Executes a get for <key>.") { key =>
    println(s"Get with $key");

    val fr = service.get(key);
    out.println("Operation sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation complete! Response was: " + r.status + ", Returned value: " + r.value.getOrElse(None));
    } catch {
      case e: Throwable => logger.error("Error during get.", e);
    }
  };

  val putCommand = parsed(P("put" ~ " " ~ simpleStr ~ " " ~ simpleStr), usage = "put <key> <value>", descr = "Executes a put for <key> and <value>.") { tuple =>
    println(s"Put with $tuple")

    val fr = service.put(tuple._1, tuple._2);
    out.println("Operation sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation complete! Response was: " + r.status);
    } catch {
      case e: Throwable => logger.error("Error during put.", e);
    }
  };

  val casCommand = parsed(P("cas" ~ " " ~ simpleStr~ " " ~ simpleStr~ " " ~ simpleStr), usage = "cas <key> <oldValue> <newValue>", descr = "Executes put <key> <newValue> if get <key> equals <oldValue>") { tuple =>
    println(s"Cas with $tuple");

    val fr = service.cas(tuple._1, tuple._2, tuple._3);
    out.println("Operation sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation complete! Response was:" + r.status + ", Returned value: "+ r.value.getOrElse(None));
    } catch {
      case e: Throwable => logger.error("Error during op.", e);
    }
  };

  val debugCommand = parsed(P("debug" ~ " " ~ simpleStr), usage = "debug <debugCodeID>", descr = "Returns the debug info associated with <debugCodeID>") { debugCodeID =>
    println(s"Debug with $debugCodeID");

    val fr = service.debug(debugCodeID);
    out.println("Operation sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds)
      out.println("Operation complete! Response was: " + r.status + ", Returned value: "+ r.value);
    } catch {
      case _: Throwable => // the promise  always throws an exception... let's not print it
    }
  };

  val benchmarkCommand = parsed(P("benchmark" ~ " " ~ simpleStr), usage = "benchmark <num>", descr = "Sends Get 0 <num> times and counts how long it takes") { num =>
    val startingTime = System.currentTimeMillis()
    var lastResponse: Option[Future[OpResponse]] = None
    for(i <- 1 to num.toInt){
      lastResponse = Some(service.get("0"))
    }
    try {
      val r = Await.result(lastResponse.get, 100.seconds)
      val finishTime = System.currentTimeMillis()
      val secDiff = (finishTime - startingTime) / 1000
      println("BENCHMARK COMPLETE. Store replied to "+num+ " get commands in "+secDiff + "seconds")
    } catch {
      case _: Throwable => // the promise  always throws an exception... let's not print it
    }
  };

}
