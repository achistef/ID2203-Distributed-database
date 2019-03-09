package se.kth.id2203.failuredetector

import se.kth.id2203.kompicsevents._
import se.kth.id2203.kvstore.Debug
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.timer.{ScheduleTimeout, Timer}
import se.sics.kompics.{ComponentDefinition => _, Port => _}

class EventuallyPerfectFailureDetector extends Port {
  indication[Suspect];
  indication[Restore];
  request[StartDetector];
}

class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {

  //EPFD subscriptions
  val timer: PositivePort[Timer] = requires[Timer];
  val pLink: PositivePort[Network] = requires[Network];
  val epfd: NegativePort[EventuallyPerfectFailureDetector] = provides[EventuallyPerfectFailureDetector];

  // EPDF component state and initialization

  //configuration parameters
  val self: NetAddress = epfdInit match {
    case Init(s: NetAddress) => s
  };
  val delta: Long = cfg.getValue[Long]("id2203.project.failureDetectorInterval");
  var myPartitionTopology: List[NetAddress] = List.empty;
  var systemTopology: Option[LookupTable] = None
  //mutable state
  var period: Long = cfg.getValue[Long]("id2203.project.failureDetectorInterval");
  var alive: Set[NetAddress] = Set[NetAddress]();
  var suspected: Set[NetAddress] = Set[NetAddress]();
  var seqnum = 0;
  var debugSource: Option[NetAddress] = None
  var responding: Boolean = true

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  timer uponEvent {
    case CheckTimeout(_) => handle {
      if (alive.intersect(suspected).nonEmpty) {
        period += delta;
      }
      seqnum = seqnum + 1;
      for (p <- myPartitionTopology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          suspected += p;

          if (responding) {
            println(s"$self EFPD suspected $p")
            trigger(Suspect(p) -> epfd)
            // send suspect to debug source
            if (debugSource.isDefined)
              trigger(NetMessage(self, debugSource.get, Suspect(p)) -> pLink)
          }
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
          if (responding) {
            println(s"$self EFPD restored $p")
            trigger(Restore(p) -> epfd);
          }
        }
        if (responding) {
          trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> pLink);
        }

      }
      alive = Set[NetAddress]();
      startTimer(period);
    }
  }

  pLink uponEvent {

    case NetMessage(src, deb@Debug("Suicide", _, _)) => handle {
      responding = false
      println(src)
      println(deb)
      println(self + " WILL STOP RESPONDING...")
      println()
    }
    case NetMessage(_, Debug("FailureDetect", source, _)) => handle {
      debugSource = Some(source)
    }

    case NetMessage(src, HeartbeatRequest(seq)) if responding => handle {
      trigger(NetMessage(self, src.src, HeartbeatReply(seq)) -> pLink);
    }
    case NetMessage(src, HeartbeatReply(seq)) => handle {
      if (seq == seqnum || suspected.contains(src.src))
        alive = alive + src.src;
    }
  }

  epfd uponEvent {
    case StartDetector(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) => handle {
      systemTopology = lookupTable
      myPartitionTopology = nodes.toList;
      suspected = Set[NetAddress]();
      alive = myPartitionTopology.toSet
      startTimer(period);
    }
  }

};


