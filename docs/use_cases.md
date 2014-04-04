# Use Case: Manatee

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
