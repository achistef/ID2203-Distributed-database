package se.kth.id2203.failuredetector
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network._
import se.sics.kompics.sl.{Init, _}
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start, ComponentDefinition => _, Port => _}


case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);
case class HeartbeatReply(seq: Int) extends KompicsEvent;
case class HeartbeatRequest(seq: Int) extends KompicsEvent;


class EventuallyPerfectFailureDetector extends Port {
  indication[Suspect];
  indication[Restore];
  request[StartDetector];
}

case class Suspect(process: Address) extends KompicsEvent;
case class Restore(process: Address) extends KompicsEvent;
case class StartDetector(nodes: Set[NetAddress]) extends KompicsEvent;


//Define EPFD Implementation
class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {

  //EPFD subscriptions
  val timer = requires[Timer];
  val pLink = requires[Network];
  val epfd = provides[EventuallyPerfectFailureDetector];

  // EPDF component state and initialization

  //configuration parameters
  val self = epfdInit match {case Init(s: NetAddress) => s};
  var topology: List[NetAddress] = List.empty;
  val delta = cfg.getValue[Long]("id2203.project.keepAlivePeriod");

  //mutable state
  var period = cfg.getValue[Long]("id2203.project.keepAlivePeriod");
  var alive = Set[NetAddress]();
  var suspected = Set[NetAddress]();
  var seqnum = 0;

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  //EPFD event handlers
  ctrl uponEvent {
    case _: Start => handle {
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => handle {
      if (!alive.intersect(suspected).isEmpty) {
        period += delta;
      }
      seqnum = seqnum + 1;
      for (p <- topology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          suspected += p;
          println(s"-----------------------EFPD suspected $p");
          trigger(Suspect(p) -> epfd);
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
          trigger(Restore(p) -> epfd);
        }
        trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> pLink);
      }
      alive = Set[NetAddress]();
      startTimer(period);
    }
  }

  pLink uponEvent {
    case NetMessage(src, HeartbeatRequest(seq)) => handle {
      trigger(NetMessage(src, HeartbeatRequest(seq)) -> pLink);
    }
    case NetMessage(src, HeartbeatReply(seq)) => handle {
      if (seq == seqnum && suspected.contains(src.src))
        alive += src.src;
    }
  }

  epfd uponEvent {
    case StartDetector(nodes: Set[NetAddress]) => handle {
      // start detector for the nodes in particular partition
      startTimer(period);
      topology = nodes.toList;
      suspected = Set[NetAddress]();
      alive = Set(topology: _*);
      seqnum = 0;
      println("/////////////////////////////////");
      println(s"STARTED EPFD FOR PARTITION $nodes");
      println("/////////////////////////////////");
    }
  }

};