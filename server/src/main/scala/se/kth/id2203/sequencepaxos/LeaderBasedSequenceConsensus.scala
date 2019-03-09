package se.kth.id2203.sequencepaxos

import se.kth.id2203.kompicsevents.{Accept, _}
import se.kth.id2203.kvstore.{Cas, Get, Op, Put}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timer}

import scala.collection.mutable;

class SequenceConsensus extends Port {
  request[SC_Propose];
  indication[SC_Decide];
  request[StartSequenceCons];
}

object State extends Enumeration {
  type State = Value;
  val PREPARE, ACCEPT, UNKOWN = Value;
}

object Role extends Enumeration {
  type Role = Value;
  val LEADER, FOLLOWER = Value;
}


class SequencePaxos extends ComponentDefinition {

  import Role._
  import State._

  val sc: NegativePort[SequenceConsensus] = provides[SequenceConsensus];
  val ble: PositivePort[BallotLeaderElection] = requires[BallotLeaderElection];
  val pl: PositivePort[Network] = requires[Network];
  val timer: PositivePort[Timer] = requires[Timer];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  val acks = mutable.Map.empty[NetAddress, (Long, List[Op])];
  val timeUnit: Int = cfg.getValue[Int]("id2203.project.timeUnit");
  val ro: Float = cfg.getValue[Float]("id2203.project.ro");
  val leasePeriod: Int = 20;
  var self: NetAddress = cfg.getValue[NetAddress]("id2203.project.address");
  var isLeaseEnabled: Boolean = cfg.getValue[Boolean]("id2203.project.enableLease");
  var pi: Set[NetAddress] = Set[NetAddress]()
  var others: Set[NetAddress] = Set[NetAddress]()
  var majority: Int = (pi.size / 2) + 1;
  var state: (Role.Value, State.Value) = (FOLLOWER, UNKOWN);
  var nL = 0l;
  var nProm = 0l;
  var leader: Option[NetAddress] = None;
  var na = 0l;
  var va = List.empty[Op];
  var ld = 0;
  // leader state
  var propCmds = List.empty[Op];
  var lc = 0;
  // Leader lease
  var tProm: Int = 0;
  var currentTime: Int = 0;
  var tL: Int = 0;
  var canRead: Boolean = false;
  var pending: Boolean = false;

  timer uponEvent {
    case CheckTimeout(_) => handle {
      currentTime += timeUnit;
      val maxTime = Math.floor(leasePeriod * (1 - ro));
      val secondsBefore = 5;
      if (leader.isDefined && self == leader.get) {
        if (pending || currentTime - tL == maxTime - secondsBefore) { // try and extend lease
          println("Trying to extend lease....");
          tL = currentTime;
          state = (LEADER, PREPARE);
          acks.clear;
          for (p <- pi - self) {
            trigger(NetMessage(self, p, Prepare(nL, ld, na, pending)) -> pl);
          }
          acks += ((self, (na, suffix(va, ld))))
          nProm = nL;
        } else if (currentTime - tL == maxTime) { // lease expired
          canRead = false;
          println("Lease expired...");
        }
      }
      startTimer();
    }
  }

  ble uponEvent {
    case BLE_Leader(l, n) => handle {
      if (n > nL) {
        leader = Some(l);
        println(s"NEW LEADER ELECTED: $leader");
        nL = n;
        if (self == l && nL > nProm) {
          state = (LEADER, PREPARE);
          propCmds = List.empty[Op];
          for (p <- pi) {
            las += ((p, 0))
          }
          lds.clear;
          acks.clear;
          lc = 0;
          tL = currentTime;
          for (p <- pi - self) {
            trigger(NetMessage(self, p, Prepare(nL, ld, na, true)) -> pl);
          }
          acks += ((l, (na, suffix(va, ld))))
          lds += ((self, ld))
          nProm = nL;
          pending = true;
        } else {
          state = (FOLLOWER, state._2);
        }
      }
    }
  }

  pl uponEvent {
    case NetMessage(p, Prepare(np, ldp, n, newLeader)) => handle {
      if (!newLeader || ((tProm == 0 || currentTime - tProm > leasePeriod * (1 + ro)) && nProm < np)) { //todo ??
        nProm = np;
        state = (FOLLOWER, PREPARE);
        var sfx = List.empty[Op];
        if (na >= n) {
          sfx = suffix(va, ldp);
        }
        tProm = currentTime;
        trigger(NetMessage(p.dst, p.src, Promise(np, na, sfx, ldp)) -> pl);
      } else {
        println("Refuse PREPARE; lease is not over");
      }
    }
    case NetMessage(a, Promise(n, na, sfxa, lda)) => handle {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        acks += ((a.src, (na, sfxa)))
        lds += ((a.src, lda))
        val P = pi.filter(acks isDefinedAt _);
        val Psize = P.size;
        if (P.size == majority) {
          val (k, sfx) = acks.maxBy { case (add, (v, comm)) => v };
          va = prefix(va, ld) ++ sfx._2 ++ propCmds;
          las(self) = va.size
          propCmds = List.empty[Op];
          state = (LEADER, ACCEPT);
          canRead = true;
          pending = false;
          println("Lease acquired....");
          for (p <- pi) {
            if ((lds isDefinedAt p) && p != self) {
              val sfxp = suffix(va, lds(p));
              trigger(NetMessage(self, p, AcceptSync(nL, sfxp, lds(p))) -> pl);
            }
          }
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        lds(a.src) = lda;
        var sfx = suffix(va, lds(a.src));
        trigger(NetMessage(self, a.src, AcceptSync(nL, sfx, lds(a.src))) -> pl);
        if (lc != 0) {
          trigger(NetMessage(self, a.src, Decide(ld, nL)) -> pl);
        }
      }
    }
    case NetMessage(p, AcceptSync(nL, sfx, ldp)) => handle {
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
        na = nL;
        va = prefix(va, ldp) ++ sfx;
        trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> pl);
        state = (FOLLOWER, ACCEPT);
      }
    }
    case NetMessage(p, Accept(nL, c: Op)) => handle {
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
        va :+= c;
        trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> pl);
      }
    }
    case NetMessage(h, Decide(l, nL)) => handle {
      if (nProm == nL) {
        while (ld < l) {
          trigger(SC_Decide(va(ld)) -> sc);
          ld += 1;
        }
      }
    }
    case NetMessage(a, Accepted(n, m)) => handle {
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        las(a.src) = m;
        val P = pi.filter(las isDefinedAt _).filter(las(_) >= m);
        if (lc < m && P.size >= majority) {
          lc = m;
          for (p <- pi) {
            if (lds isDefinedAt p) {
              trigger(NetMessage(self, p, Decide(lc, nL)) -> pl);
            }
          }
        }
      }
    }
  }

  sc uponEvent {
    case SC_Propose(c: Get) => handle {
      if (state._1 == LEADER) {
        if (canRead) {
          println("Lease is on; can perform operation locally");
          trigger(SC_Decide(c) -> sc);
        } else {
          serveOperation(c);
        }
      } else {
        println(s"Not the leader - ignore message");
      }

    }
    case SC_Propose(c: Put) => handle {
      serveOperation(c);
    }
    case SC_Propose(c: Cas) => handle {
      serveOperation(c);
    }
    case StartSequenceCons(nodes: Set[NetAddress]) => handle {
      pi = nodes;
      majority = pi.size / 2 + 1;
      if (isLeaseEnabled) {
        currentTime = 0;
        startLeaseMechanism();
      }
      trigger(StartElection(nodes) -> ble);
    }
  }

  def suffix(s: List[Op], l: Int): List[Op] = {
    s.drop(l)
  }

  def prefix(s: List[Op], l: Int): List[Op] = {
    s.take(l)
  }

  def startLeaseMechanism(): Unit = {
    startTimer();
  }

  def startTimer(): Unit = {
    val scheduledTimeout = new ScheduleTimeout(timeUnit * 1000);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  def serveOperation(c: Op): Unit = {
    if (state == (LEADER, PREPARE)) {
      propCmds :+= c;
    }
    else if (state == (LEADER, ACCEPT)) {
      va :+= c;
      las(self) += 1;
      for (p <- pi) {
        if ((lds isDefinedAt p) && p != self) {
          trigger(NetMessage(self, p, Accept(nL, c)) -> pl);
        }
      }
    } else {
      println(s"Not the leader - ignore message");
    }
  }
}
