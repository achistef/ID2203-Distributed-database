package se.kth.id2203.beb

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network.{Address, Network}
import se.sics.kompics.sl.{ComponentDefinition, Init, Port, handle}

import scala.collection.immutable.Set

case class BEB_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;

case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;

case class SetTopology(lut: Option[LookupTable], nodes: Set[NetAddress]) extends KompicsEvent;

class BestEffortBroadcast extends Port {
  indication[BEB_Deliver];
  request[BEB_Broadcast];
}


class BasicBroadcast(bebInit: Init[BasicBroadcast]) extends ComponentDefinition {
  val pLink = requires[Network];
  var beb = provides[BestEffortBroadcast];

  var self = bebInit match {
    case Init(s: NetAddress) => s
  };
  var myPartitionTopology: List[NetAddress] = List.empty;
  var systemTopology: Option[LookupTable] = None

  beb uponEvent {
    case x: BEB_Broadcast => handle {
      for (q <- myPartitionTopology) {
        trigger(NetMessage(self, q, x) -> pLink);
      }
    }

    case SetTopology(lookupTable: Option[LookupTable], nodes: Set[NetAddress]) => handle {
      systemTopology = lookupTable
      myPartitionTopology = nodes.toList;
    }
  }


  pLink uponEvent {
    case NetMessage(src, BEB_Broadcast(payload)) => handle {
      trigger(BEB_Deliver(src.src, payload) -> beb);
    }
  }
}


