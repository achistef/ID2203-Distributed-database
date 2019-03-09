package se.kth.id2203.kompicsevents

import se.kth.id2203.kvstore.Op
import se.kth.id2203.networking.NetAddress
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.KompicsEvent
import se.sics.kompics.timer.{ScheduleTimeout, Timeout}

import scala.collection.immutable.Set

case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);

case class HeartbeatReply(seq: Int) extends KompicsEvent;

case class HeartbeatRequest(seq: Int) extends KompicsEvent;

case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;

case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;

case class StartElection(nodes: Set[NetAddress]) extends KompicsEvent;

case class BLE_Leader(leader: NetAddress, ballot: Long) extends KompicsEvent;

case class Suspect(process: NetAddress) extends KompicsEvent;

case class Restore(process: NetAddress) extends KompicsEvent;

case class StartDetector(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) extends KompicsEvent;

case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;

case class Promise(nL: Long, na: Long, suffix: List[Op], ld: Int) extends KompicsEvent;

case class AcceptSync(nL: Long, suffix: List[Op], ld: Int) extends KompicsEvent;

case class Accept(nL: Long, c: Op) extends KompicsEvent;

case class Accepted(nL: Long, m: Int) extends KompicsEvent;

case class Decide(ld: Int, nL: Long) extends KompicsEvent;

case class StartSequenceCons(nodes: Set[NetAddress]) extends KompicsEvent;

case class SC_Propose(value: Op) extends KompicsEvent;

case class SC_Decide(value: Op) extends KompicsEvent;

case class BEB_Deliver(src: NetAddress, payload: KompicsEvent) extends KompicsEvent;

case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

case class BEB_Broadcast_Global(payload: KompicsEvent) extends KompicsEvent

case class SetTopology(lut: Option[LookupTable], nodes: Set[NetAddress]) extends KompicsEvent;