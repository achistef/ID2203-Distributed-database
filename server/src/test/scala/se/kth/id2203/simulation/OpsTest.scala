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
import java.util.Random

import se.sics.kompics.simulator.adaptor.Operation1
import se.sics.kompics.simulator.events.system.StartNodeEvent
import se.sics.kompics.simulator.result.SimulationResultSingleton
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.{SimulationResult, _}
import se.sics.kompics.simulator.{SimulationScenario => JSimulationScenario}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class OpsTest extends FlatSpec with Matchers {

  private val nMessages = 10

  "Get operation with <key>=[0,10]" should "return pre-loaded values 10-<key>" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = SimpleScenario.scenario(6, SimpleScenario.startClientOp1)
    SimulationResultSingleton.getInstance()
    SimulationResult += ("debugCode1" -> nMessages)
    simpleBootScenario.simulate(classOf[LauncherComp])
    for (i <- 0 to nMessages) {
      SimulationResult.get[String]("message"+i.toString).get shouldBe (10-i).toString
    }
  }

  "Processes" should "have the same correct view of partitions" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val serversCount = 6
    val delta = 3 // cannot read value with cfg
    val partitionsCount = serversCount / delta
    val simpleBootScenario = SimpleScenario.scenario(serversCount, SimpleScenario.startClientOp2)
    SimulationResult += ("debugCode2" -> "ExtractPartitionInfo")
    simpleBootScenario.simulate(classOf[LauncherComp])

    val lutListBuffer = mutable.ListBuffer.empty[String]
    for(i <- 1 to serversCount){
      val r = SimulationResult.get[String]("debugCode2"+i).get
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
    val lut = SimpleScenario.stringToLookupTable(lutList.head)
    lut.size shouldBe partitionsCount

    // partitions should not be the same
    for(i<-  lut.indices ; j<- i+1 until lut.size){
      lut(i) != lut(j) shouldBe true
    }

    // partitions should have exactly delta addresses
    for(partition <- lut) partition.size shouldBe delta

    // processes should be in exactly one partition
    for(i <- lut.indices){
      val partition = lut(i)
      for(addr <- partition ; k <- lut.indices if k!=i){
        lut(k).contains(addr) shouldBe false
      }
    }

  }

  "Broadcast messages" should "reach all servers" in {
    val seed = 123l
    val serversCount = 6
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = SimpleScenario.scenario(serversCount, SimpleScenario.startClientOp3)
    SimulationResult += ("debugCode3" -> "BroadcastFlood")
    simpleBootScenario.simulate(classOf[LauncherComp])

    val bcrListBuffer = mutable.ListBuffer.empty[String]
    for(i <- 1 to serversCount){
      val r = SimulationResult.get[String]("debugCode3"+i).get
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
    val bcastAddr = bcastReplyList.map(_.replace("BroadcastReply",""))

    //make sure all addresses are unique
    for(i<- bcastAddr.indices ; j<- i+1 until bcastAddr.size){
      bcastAddr(i) != bcastAddr(j) shouldBe true
    }

  }

  "EP Failure detectors" should "report crashed nodes" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val serversCount = 6
    val delta = 3
    val simpleBootScenario = SimpleScenario.scenario(serversCount, SimpleScenario.startClientOp4)
    SimulationResult += ("debugCode4" -> "FailureDetect")
    simpleBootScenario.simulate(classOf[LauncherComp])

    val listBuffer = ListBuffer.empty[String]
    for(i <- 1 until delta){
      val r = SimulationResult.get[String]("debugCode4"+i).get
      listBuffer += r
    }
    val replies = listBuffer.toList

    //replies count should be delta - 1
    replies.size shouldBe delta-1

    //check validity
    replies.foreach(_ contains "suspects" shouldBe true)

    val headers = replies.map(_.replace("suspects","")).map(_ split ':')
    val senders = headers.map(_ head)
    val suspect = headers.map(_.last)

    // suspect the same process
    for(i <- 0 until suspect.size - 1) suspect(i) shouldBe suspect(i+1)
    // different senders
    for(i<- senders.indices ; j <- i+1 until senders.size) senders(i) != senders(j) shouldBe true

  }

  "Put/Get operation in the KV store" should "be functioning" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = SimpleScenario.scenario(6, SimpleScenario.startClientOp5)
    SimulationResultSingleton.getInstance()

    val range = 100 to 110
    SimulationResult += ("debugCode5" -> "100-110")
    simpleBootScenario.simulate(classOf[LauncherComp])
    for (i <- range) {
      SimulationResult.get[String]("put/get:"+i.toString).get shouldBe i.toString
    }
  }

  "Put/Cas/Get operation in the KV store" should "be functioning" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = SimpleScenario.scenario(6, SimpleScenario.startClientOp6)
    SimulationResultSingleton.getInstance()

    val range = 200 to 210
    SimulationResult += ("debugCode6" -> "200-210")
    simpleBootScenario.simulate(classOf[LauncherComp])
    for (i <- range) {
      SimulationResult.get[String]("put/cas/get:"+i.toString).get shouldBe
        (if(i%2 == 0) i.toString else (-i).toString)
    }
  }


}

object SimpleScenario {

  import Distributions._
  // needed for the distributions, but needs to be initialised after setting the seed
  implicit val random: Random = JSimulationScenario.getRandom

  def stringToLookupTable(str : String): List[List[NetAddress]] = {
    str.split('|').map(stringToPartition).toList
  }

  def stringToPartition(str: String) : List[NetAddress] = {
    str.replace("Set", "")
      .replace("(", "").replace(")", "")
      .replace("NetAddress", "").replace("/", "")
      .replace(",", "").split(' ').map(_ split ':')
      .map(arr => NetAddress(InetAddress.getByName(arr(0)), arr(1).toInt)).toList
  }

  private def intToServerAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.0." + i), 45678)
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }
  private def intToClientAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.1." + i), 45678)
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def isBootstrap(self: Int): Boolean = self == 1

  val startServerOp = Op { self: Integer =>

    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr)
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1))
    }
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  }

  val startClientOp1 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1))
    StartNode(selfAddr, Init.none[ScenarioClient1], conf);
  }

  val startClientOp2 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1))
    StartNode(selfAddr, Init.none[ScenarioClient2], conf);
  }

  val startClientOp3 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1))
    StartNode(selfAddr, Init.none[ScenarioClient3], conf);
  }

  val startClientOp4 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1))
    StartNode(selfAddr, Init.none[ScenarioClient4], conf);
  }

  val startClientOp5 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1))
    StartNode(selfAddr, Init.none[ScenarioClient5], conf);
  }

  val startClientOp6 = Op { self: Integer =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1))
    StartNode(selfAddr, Init.none[ScenarioClient6], conf);
  }

  def scenario(servers: Int, cl: Operation1[StartNodeEvent, Integer]): JSimulationScenario = {

    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second))
    val startClients = raise(1, cl, 1.toN).arrival(constant(1.second))

    startCluster andThen
      10.seconds afterTermination startClients andThen
      100.seconds afterTermination Terminate
  }

}
