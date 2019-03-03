package se.kth.id2203.sequencepaxos

import java.util.UUID

import se.kth.id2203.kompicsevents.{Accept, _}
import se.kth.id2203.kvstore.{Op, StopSign}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl._

import scala.collection.mutable
import scala.math.Ordering.Implicits._

class SequenceConsensus extends Port {
  request[SC_Propose];
  indication[SC_Decide];
  request[StartSequenceCons];
}

object State extends Enumeration {
  type State = Value;
  val PREPARE, ACCEPT, UNKOWN, STOPPED = Value;
}

object Role extends Enumeration {
  type Role = Value;
  val LEADER, FOLLOWER = Value;
}

class SequencePaxos extends ComponentDefinition {

  import Role._
  import State._

  // implements
  val sc = provides[SequenceConsensus];
  // requires
  val ble = requires[BallotLeaderElection];
  val pl = requires[Network];


  var i = 0l
  val ci = 0l;    // configuration of the replica
  var pi = Set[NetAddress]()    // set of processes in configuration
  var replica:Set[NetAddress] = Set[NetAddress]()    // set of replicas in ci
  var state: (Role.Value, State.Value) = (FOLLOWER, UNKOWN);    // role and phase state
  var finalSeq = List.empty[Op] // final sequence from prev config
  // proposer state
  var nL = (i, 0l);    // leaders (round, number)
  val acks = mutable.Map.empty[NetAddress, (Long, List[Op])];   // promises
  val las = mutable.Map.empty[NetAddress, Int];   // length of longest accepted sequence per acceptor
  val lds = mutable.Map.empty[NetAddress, Int];   // length of longest known sequence per acceptor
  // acceptor
  var propCmds = List.empty[Op];    // set of commands that need to be appended to the log
  var lc = 0;   // length of the longest chosen sequence
  // acceptor state
  var nProm = (i, 0l);   // promise not to accept lower rounds
  var na = 0l;    // round number
  var va = List.empty[Op];    // sequence accepted
  // learner state
  var ld = 0;   // length of decided sequence


  var self = cfg.getValue[NetAddress]("id2203.project.address");
  var others = Set[NetAddress]()
  var majority: Int = (pi.size / 2) + 1;
  var leader: Option[NetAddress] = None;

  val SS: Op = StopSign(self)

  def stopped(): Boolean = {
    if (va.nonEmpty) {
      if (va(ld) == SS) {
        return true
      }
    }
    false
  }


  ble uponEvent {
    case BLE_Leader(l, n) => handle {
      var b = (i, n)
      if (b > nL) {
        leader = Some(l);
        println(s"NEW LEADER ELECTED: $leader");
        nL = b
        //nProm = b
        if (self == l && nL > nProm) {
          println(s"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<$self: I'm the leader!")
          state = (LEADER, PREPARE);

          propCmds = List.empty[Op];
          for (p <- pi) {
            las += ((p, 0))
          }
          lds.clear;
          acks.clear;
          lc = finalSeq.size;
          for (p <- pi - self) {
            trigger(NetMessage(self, p, Prepare(nL, ld, na)) -> pl);
          }
          acks += ((l, (na, suffix(va, ld))))
          lds += ((self, ld))
          nProm = nL;
        } else {
          state = (FOLLOWER, state._2);
        }
      }
    }
  }

  pl uponEvent {
    case NetMessage(p, Prepare(np, ldp, n)) => handle {
      if (nProm < np) {
        nProm = np;
        state = (FOLLOWER, PREPARE);
        var sfx = List.empty[Op];
        if (na >= n) {
          sfx = suffix(va, ldp);
        }
        trigger(NetMessage(p.dst, p.src, Promise(np, na, sfx, ldp)) -> pl);
      }
    }
    case NetMessage(a, Promise(n, na, sfxa, lda)) => handle {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        acks += ((a.src, (na, sfxa)))
        lds += ((a.src, lda))
        val P = pi.filter(acks isDefinedAt _);
        val Psize = P.size;
        if (P.size == majority) {
          var (k, sfx) = acks.maxBy { case (add, (v, comm)) => v };
          va = prefix(va, ld) ++ sfx._2 ++ propCmds;
          // SS in va

          if (va.nonEmpty) {
            if (va.last == SS) {
              propCmds = List.empty[Op]
            } else if (propCmds.nonEmpty) {
              if (propCmds.contains(SS)) {
                println("Dropping commands due to SS")
              } else {
                for (c <- propCmds) {
                  if (propCmds.contains(c)) {
                    va :+= c
                  }
                }
              }
            }

          }

          las(self) = va.size
          //propCmds = List.empty[Op];
          state = (LEADER, ACCEPT);
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
        na = nL._2;
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
          //println(s"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< $va")
          trigger(SC_Decide(va(ld)) -> sc);
          ld += 1;
        }
      }
    }
    case NetMessage(a, Accepted(n, m)) => handle {
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        las(a.src) = m;
        var P = pi.filter(las isDefinedAt _).filter(las(_) >= m);
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
    case SC_Propose(c: Op) => handle {
      println("RECEIVED SC_RESPONSE FROM ", c.source);
      if (state == (LEADER, PREPARE)) {
        propCmds :+= c;
      }
        // added this
      else if (state == (LEADER, ACCEPT) && !stopped()) {
        va :+= c;
        //las(self) += 1;
        las(self) += va.size;
        for (p <- pi) {
          if ((lds isDefinedAt p) && p != self) {
            trigger(NetMessage(self, p, Accept(nL, c)) -> pl);
          }
        }
      } else {
          println(s"--------------Not the leader - ignore message");
      }
    }
    case StartSequenceCons(nodes: Set[NetAddress]) => handle {
      pi = nodes;
      majority = pi.size / 2 + 1;
      trigger(StartElection(nodes) -> ble);
    }
  }

  def suffix(s: List[Op], l: Int): List[Op] = {
    s.drop(l)
  }

  def prefix(s: List[Op], l: Int): List[Op] = {
    s.take(l)
  }
}