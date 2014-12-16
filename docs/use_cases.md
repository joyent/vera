<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

## Use Cases

This is meant to outline how the [interface](interface.md) maps to use cases.
If you aren't yet familiar with the Vera interface, you should read that before
reading this.

## Use Case: Leader Election / Service Topology

Vera is used by services to elect a leader from some number of independent
peers.  In this example, we'll use three distinct peers, `10.77.77.6`,
`10.77.77.8` and `10.77.77.12`, all vying to be a leader.  Also, one will be
designated a synchronous peer.

To begin, all peers are off.  When they turn on, they each connect to Vera and
send a `push` request at some path that they are configured with.  For example,
each would:

```
{
    "action": "push",
    "path": "/data/db/topology",
    "data": {
        "zoneId": "b826d702-aad9-473c-9049-a0b8ef302f8c",
        "ip": "10.77.77.8",
        "conn": "tcp://postgres@10.77.77.8:5432/postgres"
    }
}
```

Since data is strictly ordered in Raft, one peer will arrive first.  This first
peer will be designated the "leader".  When the second peer arrives, the first
peer will be sent a notification that there is now a second item in the list.
When the third arrives, the first and second will be notified that there is a
third item in the list.  The strict ordering of the items in the list gives the
topology for the three peers.  The first is the leader and the second is the
synchronous peer.  The list will look something like:

```
{
    "items": [
        {
            "index": 1233,
            "owner": 10.77.77.8,
            "data": {
               "zoneId": "b826d702-aad9-473c-9049-a0b8ef302f8c",
               "ip": "10.77.77.8",
               "conn": "tcp://postgres@10.77.77.8:5432/postgres"
            }
        }, {
            "index": 1234,
            "owner": 10.77.77.6,
            "data": {
               "zoneId": "c2fce557-039f-46a8-86a2-1fd9d2323846",
               "ip": "10.77.77.6",
               "conn": "tcp://postgres@10.77.77.6:5432/postgres"
            }
        }, {
            "index": 1235,
            "owner": 10.77.77.12,
            "data": {
               "zoneId": "0f7b4f94-e72d-4709-9be3-a90a8c69f264",
               "ip": "10.77.77.12",
               "conn": "tcp://postgres@10.77.77.12:5432/postgres"
            }
        }
    ]
}
```

The cluster can use that information to configure itself.  At some point, one
of the hosts will go away.  Perhaps from a network partition or from a hardware
failure.  When that happens, Vera will detect that the host goes away, removes
the item from the list, and notifies the remaining peers that an element has
been removed.  Using the above example, say that `10.77.77.8`, the current
leader, goes away.  `10.77.77.6` and `10.77.77.12` are notified that the master
has been removed, `10.77.77.6` is now at the top of the list, meaning it is
now the master, and `10.77.77.12` is promoted to the synchronous peer.  When
`10.77.77.8` comes back online, it will reconnect with Vera and receive the
notifications it missed while away, one of which will be that the data it was
holding has been purged.  It can then send a new `push` request, and be added
back in the topology at the end of the list.

It's also noteworthy that there may be some number of hosts that need to know
when the leader has changed.  Using the above example, say that the three
peers are database hosts.  Each of the clients to that database cluster should
know when the leader (master database) changes, so that writes can be directed
to the appropriate place.  Each of these clients would register a watch on
the election path, as above:

```
{
    "action": "watch",
    "path": "/data/db/topology"
}
```

Then those clients will also receive notifications when the leader changes.

## Use Case: DNS

Vera can be used as a DNS system.  There are two types of clients to Vera in
this case, the DNS servers and the clients wishing to register themselves in
DNS.

Each client that wishes to register itself for a name in DNS will keep a
connection open with Vera and push data onto some number of lists, depending
on the desired DNS entries.  For example, say that all index hosts should be
advertised in a DNS A record, as well as having a CNAME.  They might push the
following onto two lists:
```
{
    "action": "push",
    "path": "/data/dns/us/joyent/coal/moray/1",
    "data": {
        "zoneId": "4800f942-b569-4e05-b376-0ed9d5367bca",
        "ip": "10.77.77.9",
        "type": "A",
        "ttl": 30
    }
}
...
{
    "action": "push",
    "path": "/data/dns/us/joyent/coal/4800f942",
    "data": {
        "zoneId": "4800f942-b569-4e05-b376-0ed9d5367bca",
        "ip": "10.77.77.9",
        "type": "CNAME",
        "ttl": 30
    }
}
```

When the client goes away, the previous data will be purged from Vera.  Since
DNS can be cached, we can assume that eventually consistent reads are OK.  The
DNS servers could be any process that knows how to HTTP GET.  For example, if
the DNS server got a request for `1.moray.coal.joyent.us`. it would reverse the
path and do an HTTP get to Vera at `GET /data/dns/us/joyent/coal/moray/1
HTTP/1.1`.  It could then cache the response, randomize the entries in the list
and answer the DNS request.

## Use Case: LB Hosts

This use case is very similar to the DNS A record use case outlined previously.
Building on that example, the frontend hosts would push entries on a path that
the Load Balancer knows to watch.  For example, the frontend would push to the
following path:

```
{
    "action": "push",
    "path": "/data/frontend",
    "data": {
        "zoneId": "95ee9a76-f56c-418f-8b50-104a51e2a506",
        "ip": "10.77.77.21"
    }
}
```

Then the load balancer will set a watch on that path:

```
{
    "action": "watch",
    "path": "/data/frontend"
}
```

The first time the LB connects, it will get the current set of hosts and
configure itself to load balance between them.  When a frontend host goes away
or a new one comes online, the LB will get a notification that an element in the
list was added/removed and reconfigure itself.

## Use Case: Manatee

This details a specific instance of the Leader Election use case.

The main use case for Vera is the replacement of Zookeeper in Manta, Joyent's
distributed object store.  One use case for Zookeeper is to coordinate master
failover for a cluster of dbs.  Each of the DB processes has a separate
processes that controls its configuration and lifecycle.  Each of these
controller processes connects to Zookeeper, which ultimately determines the
cluster topology.

One issue in depending on an external system from the controller processes is
determining when it is unsafe to continue taking writes.  Consider the following
topology:

```
 Datacenter A       |    Datacenter B       |    Datacenter C
                    |                       |
 +-------------+    |    +-------------+    |    +-------------+
 | Db1, Master |    |    | Db2, Slave  |    |    | Db3, Slave  |
 +-------------+    |    +-------------+    |    +-------------+
     |                        |             |           |
     |  +---------------------+             |           |
     |  |                                               |
     |  |  +--------------------------------------------+
     |  |  |
     v  v  v        |                       |
 +-------------+    |    +-------------+    |    +-------------+
 | Vera1, Lead |    |    | Vera2, Foll |    |    | Vera3, Foll |
 +-------------+    |    +-------------+    |    +-------------+

```

With this topology, it is safe for Db1 to take writes as long as it isn't
possible for Db2 or Db3 to transition to Master.  There are several failure
scenarios that matter to the health of this cluster:

1. Db Master fails
2. Vera Leader fails
3. Network partitions off Datacenter A from B and C.

If one of any of the other components or Datacenters fail, it shouldn't affect
how the cluster is configured.  _Note: This isn't entirely true, since Manatee
depends on a synchronous slave for writes.  Manatee would need to reconfigure
the slave if the sync fails, but that shouldn't cause a new leader to be
elected.  Also, we aren't considering multiple failures since the above topology
will only tolerate multiple failures as long as they are in the same
Datacenter._

The way a cluster reacts depends on the location of the Db master and the Vera
leader.  They can either be in the same datacenter and fail together or be in
separate datacenters and fail independently.

1. Same Datacenter: The Manatee master fails along with the Vera leader (both
   are partitioned from the rest of the cluster).  In this case, Manatee will go
   down for Vera's leader election time + time for Manatees to connect to Vera
   leader + time for Manatee to elect a new master and reconfigure.  There are
   two things to consider here:
    1. How Db1 detects it is no longer the master.  The old Manatee master must
       detect that it is unsafe to continue accepting writes.  Vera will detect
       that it is no longer connected to a majority, and cuts off all consistent
       read connections.  Db1 gets cut off, goes to read-only, and tries to find
       the new Vera leader, which fails until the partition resolves.
    2. How the remaining, connected Manatees continue.  Vera followers will
       detect the leader is gone after some timeout, and go through leader
       election.  The Manatee slaves won't be able to communicate with the Vera
       leader and attempt to find the new one.  When they connect to the new
       leader, they will reconfigure.
2. Vera Leader fails: The Manatee master stays up and gets disconnected from the
   leader.  Manatee will go down for Vera's leader election time + time for
   Manatees to connect to Vera leader + time for Manatee to elect a new master
   and reconfigure.  Some things to consider:
    1. If the Manatee master knew it was a Vera-only problem, it could continue
       to act as the leader for a short period of time.  Once a new Vera leader
       is elected, the Manatee master would need some time to find and connect
       to the Vera leader before the new Vera leader timed it out.  If we could
       make this work, there would be no downtime for Manatee.  The trick, of
       course, is making Manatee aware that it is Vera's problem.
3. Manatee master fails: Vera is unaffected.  Vera leader detects that the
   Manatee master is gone, notifies the Manatee slaves that the ephemeral
   data is gone, Manatee reconfigures.  Manatee would be down for time it takes
   for Vera to detect Manatee master is gone + time for Vera to notify + time
   for Manatee to reconfigure.
