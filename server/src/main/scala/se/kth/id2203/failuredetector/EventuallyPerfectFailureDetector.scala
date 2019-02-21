package se.kth.id2203.failuredetector
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
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
case class StartDetector(lut: Option[LookupTable], nodes: Set[NetAddress]) extends KompicsEvent;


//Define EPFD Implementation
class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {

  //EPFD subscriptions
  val timer = requires[Timer];
  val pLink = requires[Network];
  val epfd = provides[EventuallyPerfectFailureDetector];

  // EPDF component state and initialization

  //configuration parameters
  val self = epfdInit match {case Init(s: NetAddress) => s};
  var myPartitionTopology: List[NetAddress] = List.empty;
  var systemTopology: Option[LookupTable] = None

  val delta = cfg.getValue[Long]("id2203.project.failureDetectorInterval");
  //mutable state
  var period = cfg.getValue[Long]("id2203.project.failureDetectorInterval");
  var alive = Set[NetAddress]();
  var suspected = Set[NetAddress]();
  var seqnum = 0;

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
          println(s"----------------------- EFPD suspected $p");
          trigger(Suspect(p) -> epfd);
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
          println(s"----------------------- EFPD restored $p");
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
      trigger(NetMessage(self, src.src, HeartbeatReply(seq)) -> pLink);
    }
    case NetMessage(src, HeartbeatReply(seq)) => handle {
      if (seq == seqnum || suspected.contains(src.src))
        alive = alive + src.src;
    }
  }

  epfd uponEvent {
    case StartDetector(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) => handle {
      // start detector for the nodes in particular partition
      systemTopology = lookupTable
      myPartitionTopology = nodes.toList;
      suspected = Set[NetAddress]();
      alive = myPartitionTopology.toSet
      startTimer(period);
    }
  }

};


