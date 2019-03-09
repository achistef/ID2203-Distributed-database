package se.kth.id2203.sequencepaxos

import se.kth.id2203.kompicsevents._
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.Start
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timer}

import scala.collection.mutable;

class BallotLeaderElection extends Port {
  indication[BLE_Leader];
}

class GossipLeaderElection extends ComponentDefinition {

  val ble = provides[BallotLeaderElection];
  val pl: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];

  val self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  val delta: Long = cfg.getValue[Long]("id2203.project.leaderElectionInterval");
  private val ballots = mutable.Map.empty[NetAddress, Long];
  private val ballotOne = 0x0100000000l;
  var topology: Set[NetAddress] = Set.empty;
  var majority = 0;
  private var period = delta;
  private var round = 0l;
  private var ballot = ballotFromNAddress(0, self);
  private var leader: Option[(Long, NetAddress)] = None;
  private var highestBallot: Long = ballot;

  def ballotFromNAddress(n: Int, adr: NetAddress): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  ctrl uponEvent {
    case _: Start => handle {
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => handle {
      if (ballots.size + 1 >= majority) {
        checkLeader();
      }
      ballots.clear;
      round += 1;
      for (p <- topology) {
        if (p != self) {
          trigger(NetMessage(self, p, HeartbeatReq(round, highestBallot)) -> pl);
        }
      }
      startTimer(period);
    }
  }

  pl uponEvent {
    case NetMessage(src, HeartbeatReq(r, hb)) => handle {
      if (hb > highestBallot) {
        highestBallot = hb;
      }
      trigger(NetMessage(self, src.src, HeartbeatResp(r, ballot)) -> pl);
    }
    case NetMessage(src, HeartbeatResp(r, b)) => handle {
      if (r == round) {
        ballots += ((src.src, b));
      } else {
        period += delta;
      }
    }
  }

  ble uponEvent {
    case StartElection(nodes: Set[NetAddress]) => handle {
      topology = nodes;
      majority = topology.size / 2 + 1;
      startTimer(period);
    }
  }

  private def checkLeader() {
    ballots += ((self, ballot));
    val (topProcess, topBallot) = ballots.maxBy(_._2);
    if (topBallot < highestBallot) {
      while (ballot <= highestBallot) {
        ballot = incrementBallotBy(ballot, 1);
      }
      leader = None;
    } else if ((leader.isDefined && (Some(topBallot, topProcess) != leader)) || leader.isEmpty) {
      highestBallot = topBallot;
      makeLeader((topBallot, topProcess));
      trigger(BLE_Leader(topProcess, topBallot) -> ble);
    }
  }

  private def makeLeader(topProcess: (Long, NetAddress)) {
    leader = Some(topProcess);
  }

  def incrementBallotBy(ballot: Long, inc: Int): Long = {
    ballot + inc.toLong * ballotOne
  }

  private def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }
}