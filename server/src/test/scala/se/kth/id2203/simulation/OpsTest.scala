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

import org.scalatest._
import se.kth.id2203.ParentComponent
import se.kth.id2203.networking._
import se.sics.kompics.network.Address
import java.net.{InetAddress, UnknownHostException}

import se.sics.kompics.simulator.adaptor.Operation1
import se.sics.kompics.simulator.events.system.StartNodeEvent
import se.sics.kompics.simulator.result.SimulationResultSingleton
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.{SimulationResult, _}
import se.sics.kompics.simulator.{SimulationScenario => JSimulationScenario}

import scala.collection.mutable
import scala.concurrent.duration._

class OpsTest extends FlatSpec with Matchers {

  private val nMessages = 10;

  "Get operation with key=[0,10]" should "return 10-key" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenario.scenario(6, SimpleScenario.startClientOp1);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String]("message"+i.toString).get shouldBe (10-i).toString;
    }
  }

  "Processes" should "have the same view of partitions" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val serversCount = 6
    val partitionsCount = 2
    val simpleBootScenario = SimpleScenario.scenario(serversCount, SimpleScenario.startClientOp2);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("debugCode1" -> "ExtractPartitionInfo");
    simpleBootScenario.simulate(classOf[LauncherComp]);

    val lutListBuffer = mutable.ListBuffer.empty[String]
    for(i <- 1 to serversCount){
      val r = SimulationResult.get[String]("debugCode1"+i).get
      lutListBuffer += r
    }

    val lutList = lutListBuffer.toList

    // check if all servers replied
    lutList.size shouldBe serversCount

    // check if all lut are equal
    for(i <- 0 until serversCount - 1){
      lutList(i) shouldBe lutList(i+1)
    }

    // there should be exactly two partitions
    val lut = lutList(0)
    val partitions = lut.split('|')
    partitions.size shouldBe partitionsCount

    // partitions should not be the same
    for(i<-  partitions.indices ; j<- i+1 until partitions.length){
      partitions(i) != partitions(j) shouldBe true
    }
    // partitions should have exactly serversCount/partitionsCount addresses
    for(partition <- partitions)
      {
        partition.split(',').count(_.contains("NetAddress")) shouldBe (serversCount/partitionsCount)
      }

  }

  "Broadcast messages" should "reach all servers" in {
    val seed = 123l;
    val serversCount = 6
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenario.scenario(serversCount, SimpleScenario.startClientOp3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("debugCode2" -> "BroadcastFlood");
    simpleBootScenario.simulate(classOf[LauncherComp]);

    val bcrListBuffer = mutable.ListBuffer.empty[String]
    for(i <- 1 to serversCount){
      val r = SimulationResult.get[String]("debugCode2"+i).get
      bcrListBuffer += r
    }
    val bcastReplyList = bcrListBuffer.toList

    //there should be exactly <serversCount> responses
    bcastReplyList.size shouldBe serversCount

    //check validity of replies
    for(str <- bcastReplyList){
      str.startsWith("BroadcastReply") shouldBe true
    }

    // keep only addresses
    val bcastValues = bcastReplyList.map(_.replace("BroadcastReply",""))

    //make sure all addresses are unique
    for(i<- bcastValues.indices ; j<- i+1 until bcastValues.size){
      bcastValues(i) != bcastValues(j) shouldBe true
    }

  }

}

object SimpleScenario {

  import Distributions._
  // needed for the distributions, but needs to be initialised after setting the seed
  implicit val random = JSimulationScenario.getRandom();

  private def intToServerAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.0." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }
  private def intToClientAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.1." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def isBootstrap(self: Int): Boolean = self == 1;

  val startServerOp = Op { self: Integer =>

    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr)
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1))
    };
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  };

  val startClientOp1 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init.none[ScenarioClient1], conf);
  };

  val startClientOp2 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init.none[ScenarioClient2], conf);
  };

  val startClientOp3 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init.none[ScenarioClient3], conf);
  };

  def scenario(servers: Int, cl: Operation1[StartNodeEvent, Integer]): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClients = raise(1, cl, 1.toN).arrival(constant(1.second));

    startCluster andThen
      10.seconds afterTermination startClients andThen
      100.seconds afterTermination Terminate
  }

}
