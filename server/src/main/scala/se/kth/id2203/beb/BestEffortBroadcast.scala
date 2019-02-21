package se.kth.id2203.beb

import se.kth.id2203.kvstore.{Debug, OpCode}
import se.kth.id2203.kompicsevents.{BEB_Broadcast, BEB_Deliver, SetTopology}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.network.{Network}
import se.sics.kompics.sl.{ComponentDefinition, Init, Port, handle}

import scala.collection.immutable.Set

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
    case msg@ BEB_Broadcast(Debug("BroadcastFlood", receiver,_))  => handle {
      for(it <- systemTopology.get.partitions; address <- it._2)
        trigger(NetMessage(self, address, msg) -> pLink)
    }

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
    case NetMessage(_, msg @ BEB_Broadcast( deb@ Debug("BroadcastFlood", receiver, _))) => handle {
      trigger(NetMessage(self, receiver, deb.response(OpCode.Ok, Some("BroadcastReply"+ self))) -> pLink)
    }

    case NetMessage(src, BEB_Broadcast(payload)) => handle {
      trigger(BEB_Deliver(src.src, payload) -> beb);
    }
  }
}


