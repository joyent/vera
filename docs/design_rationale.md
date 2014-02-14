## Introduction

This document is meant to capture some of the design rationale for how vera is
structured.  This is the place where questions about raft have been asked and
the rationale for the answers.  It's very raw (needs to be split into many
docs), so read at your own peril.

## Origin of Vera

`xmpp/logs/scrum@/2013/10/31.html#16:32:29.100837`

## Leveldb key width

Since we need to read in order of index we need to fix the width of the index
from the beginning.  There are some obvious choices that makes life easier or
harder, all of this will depend on how log a lifetime we expect from a vera
instance.  As a simple example, say that we used a single byte to represent
the index and used that as the key into the log.  When the index reached 2^8,
the log index would wrap and probably cause all sort of badness because we'd
start overwriting earlier entries.  That's a really bad thing.

So, we need to determine an upper-bound to how big an index should grow.  In
a best case, let's assume that the system takes a write rate of 1000 requests
per second for 10 years.  That's a lot of requests for a long time.  I don't
expect that a single vera system would need to last for nearly that long.
So...

1000 * 60 * 60 * 24 * 365 * 10 ~ 2^38

So javascript's built-in Buffer.writeUInt32BE isn't big enough to handle our
needs.  So it looks like we need to represent as buffers.  A big integer
library could work, but ideally I'd like to stay with javascript's native
number, which can go to up to 2^53.  To put that in perspective, it could last
for ~285,000 years at 1000 rps.

That all said... to make progress for now, I'm going to go with ^^, and will
come back to integrate a big int library.  A 32-bit int will run out after
about 50 days at 1000 rps.  So this *must* be fixed.

FYI, it now uses [bignum](https://npmjs.org/package/bignum).

## Leader Ticker

We're using the same ticker in the leader as we do in the clients, but instead
of sending the instance into candidate mode, it's used for sending heartbeats.
So, each time an appendEntries succeeds we reset the ticker since that's the
same as sending a heartbeat.

Is it ok to reset the ticker when only a majority have responded? If the
majority has responded then we're sure we have the leadership position.  Other
heartbeats will still be retried by the underlying layer.  So it should be fine.

Um.... but what about those heartbeats piling up?  Same thing with append
entries piling up or anything else.  We have to deal with it no matter what.

## Efficient Truncation with leveldb

So there's no API to truncate a leveldb (or at least, I don't see one).  The
keys are read in order, however, so we have a place that we can *simulate* a
truncation and hide it in the leveldb log implementation.  So even though there
may be entries after a certain point, they wouldn't be considered part of the
log as far as the raft implementation is concerned.  Since there's only one
object that accesses the log (the leveldb wrapper), the problem really boils
down to how it figures out the current highest log index when starting up since
it can keep it in memory otherwise.  We could find out a more efficient way,
perhaps?

For now, we write the index of the last entry.  This may cause a lot more churn
than a more clever way, but it works.

Other things I considered and rejected:
1. Having each key record the *time* it was applied on the leader.  Then when
a truncation occurs, entries *after* the truncation would have timestamps lower
than the entries that are written after the truncation.  This relies on times
being synced... so rejected.
2. The same idea as ^^, but have a "truncation generation".  This is the same
idea as ^^, except with a variable in the db.  It essentially means that the
logs between raft peers will diverge (slightly, but still).  It seems more
complicated than just keeping the last index in a well-known location.

## If the stack isn't stable, clients get the run-around...

Since a client is forwarded to the leader by followers, if the stack isn't
stable, then the client will get bounced between peers for "a long time".  How
can the client make sure that the application knows what's going on?  How
can the cluster notify the client that it is in the middle of a leader election
and to come back later?

## No "lastApplied" and "commitIndex"?

They are there.  LastApplied is the state machine's commit index.  The clog's
commitIndex is what it is named.  I figured the command log and the state
machine should keep track of where they are rather than duplicate in the raft
class.

## Ha!  You can run raft with a single instance.  That's dumb...

In production, sure, :).  But in throw-away environments or environments
where data is completely ephemeral, it's fine.

Plus, for operators, it's easier to set up one instance, add a second, add a
third, etc rather than hand-writing config files.

## Requests from an unknown peer?

There are a few options here- we can throw a 500 (internal error), return a
specific status code, or just return false.  There could be a couple reasons
this would happen in a running system:

1. An out of date peer (perhaps one that isn't aware yet that a cluster change
   has been made)
2. An old message arrives after a cluster change has been made
3. Bad actor in the system.

Seems best to return a specific error message (InvalidPeer)- and make sure to
log a message on the client.

## When the commit index is further ahead than the last log entry on a follower?

Say that a follower is really lagging, up to the point that sending huge batches
of entries isn't going to work (one could argue that in that case the follower
should just suicide and restore from a peer).  If we started limiting the number
of entries, the commit index on the master could exceed the end of the entries
sent to the clients.  The obvious fix here is that the master should just send
sendCommitIndex = Min(commitIndex, entries[-1].index);

Another option would be for the client to always respond with the commit
index after an append entries, but I'd rather not add to the protocol.

In the case a client receives an invalid commit index (one past the end) it
should reject the request.  This is problematic with streams because the
follower won't know what the last entry looks like until it gets there.  There
are a couple alternatives:

1. Leader sends lastIndex/lastTerm with the request (as separate parameters).
2. Follower reads (and applies) the stream up to the point where the error is
   detected.

Since this is an edge-case and a sanity check, #2 is probably fine rather than
polluting the API.  The downside is that it is going to apply good entries up to
the bad one... TODO: (Not sure if this is the right choice)

## What should happen after a vote is cast and a leader isn't established?

The election timeout is reset when a follower votes, so the term should be
incremented and it should transition to become a candidate.

## Append Entries API and Consistency Check

The initial implementation of the log's appendEntries request looked like this:

    appendEntries(prevLogIndex, prevLogTerm, entries)

But to pull out the prevLogIndex and prevLogTerm makes for a more complex
interface.  IMO, it's a cleaner interface to have:

    appendEntries(entries)

Where the requirement is that entries[0] must already exist in the log, exactly
as transmitted.  The downside is that, unless redacted, the first entry will
also have the command (meaning that the data will be transmitted over the wire
along with the new commands), but I view that as a bonus- that can be compared
too, as a sanity check.  It also makes for a more understandable heartbeat API.

## Eventually Consistent Client reads when a Leader Dies

Is there a case where a client will see eventually-consistent behavior if a
leader falls over and a new one starts up?

Boils down to how a new leader figures out what the current commit index is.

Seems that, yes, this is a problem:
1) Old leader completes a request, returns success to client.  It has not yet
issued append entries to followers to indicate that the lastCommitIndex has
increased.
2) Old leader dies.
3) Raft guarantees that a new leader will have that log, though the new leader
doesn't have the same lastCommitIndex as the old leader.

If the new leader were to start serving reads from the state machine at this
point, it would serve a stale read to clients up until it stores *one write* of
its current term on a majority of followers.  Since that write is client-
driven, it could be a "long time" before reads are turned back on.

Update: In the Oct 7, 2013 version of the raft paper the Client Interactions
section states that:

The Leader Completeness Property guarantees that a leader has all committed
entries, but at the start of its term, it may not know which those are. To find
out, it needs to commit an entry from its term. Raft handles this by having each
leader commit a blank no-op entry into the log at the start of its term

So we came to the same conclusion: leader must commit one entry before serving
client requests.

## Aren't there problems where append entries happen in parallel?

Can concurrent append entries requests cause issues?  Not if they have
overlapping entries.  Depends on the log implementation, I guess.  For us,
the log entries will be read many times, but has no adverse side effects.

## A small rant

From the client perspective, no matter how you implement a distributed system,
*everything* is eventually consistent.  In a system like raft, the only place
where the environment *isn't* eventually consistent is on the server acting as
leader, at the place and time when the state machine is updated.  To compensate
for this, chubby, zookeeper and etcd have all chosen to implement notifications.
There is no reason why a lock service couldn't have been written without
notifications.  The reason all of these systems have chosen to implement
notifications is because when a fundamental piece of information changes, like
when a lock is released or a db master dies, it is convenient to notify all
parties that need to know rather than them having to find out by polling the
distributed consensus service or through some other, external channel.

## State Machine Command application

There are obviously times where clients will issue a command that attempts to
apply an invalid command for the machine's state.  If the command is checked
before sending the command through raft, there is a possibility that the command
is not valid at checking time, but would be valid at application time (since
there could be in-progress commands that would change the machine's state to
valid for the command).

The problem is that it is difficult to fail-fast (except on invalid syntax).  Is
There a way to safely fail fast?  Can have a copy of the state machine that
is more up to date than the actual state machine (commands are applied after the
entry is assigned a stable index) and use that to verify commands.  Kinda
expensive, though, depending on what the state machine is (and how big it is).

Without a way to fail-fast on invalid state machine transitions, all clients
must wait for a full round of latency before finding out that they sent an
invalid command.

Could have an implicit lock on the command's target data and have all other
clients wait until request is processed before proceeding with next client
request?

Ultimately, they have to wait.  It's not safe to do it any other way.

## Command Log

Log appends are a natural (and necessary) choke-point.

Ultimately all log entries must be serialized in order, to disk.  Entries that
have lower indexes must be written before higher indexes.

Logs should have a cumulative checksum with each record, computed independently
for each replica.  Checksums should be compared along with the lastTerm and
lastIndex when appending to logs.

Index should be determined as close to the file writing as possible since in
node it isn't safe to getIndex and guarantee that the puts will remain in
order.

Alternatively, another choke point is necessary, that will be the only place
that writes to the log.

Appends should be batched- small writes to disk cause badness.

## Decreasing latency

As written, it seems that raft assumes this, minimum latency, as viewed from a
client, assuming as much parallelism as possible:
1) Network latency from client to leader PLUS
2) Disk latency Leader fsyncing command to disk PLUS
3) Network latency from leader to slowest-of-majority (SOM) follower PLUS
4) Disk latency SOM fsyncing command to disk PLUS
5) Network latency response from SOM to Leader PLUS
6) Latency Leader applying command to state machine PLUS
7) Network latency response from Leader back to client.

Is it safe to not persist to disk on the master before issuing append rpcs for
followers?  Otherwise, we're waiting for two, strictly serialized fsync()s.

Master should batch a set of appends, issue append entry rpcs, then persist to
disk.  On response from a majority of clients, it should make sure it has
persisted the data itself, then return to the client.

Is it strictly necessary that the leader must persist to its log if it is
counting itself as one of the majority?  For example, if there are three nodes,
and the two followers commit to disk and return before the leader has committed
to disk, can the leader safely apply that to the state machine and return to
the client?
- If leader fails, another leader will take the place, and it won't be the
  current leader since it can't receive a majority vote.
- Problem is only if something makes assumptions that the log is already
  consistent with the state machine.

## Clients

If a client times out waiting for an entry to be committed, what should be its
course of action?

Client observes state.  Client wishes to update state.  State can change between
when clients observe and when client attempts to apply.  What are mechanisms
that clients can use to "do the right thing"?  This needs to be solved in Vera's
api.

## Notifications

Desirable properties for notifications:

1. When clients register for a notification, they will receive all notifications
   from that time forward, until they unregister.
2. Clients do not have to re-register for the notifications they want each time
   they connect.
3. If clients disconnect for a time and re-connect, they will receive
   notifications for the things that they missed while they were down.
4. Clients will receive notifications in the order that changes occurred.
5. Clients must ack notifications.

Distributed systems 101 teaches that you can either choose at least once or at
most once.  In a system like this, at most once is not desirable.

A simple, low storage cost way to keeping track of notifications is keeping a
list of keys that a client wants notifications on, then keep the log index of
where each client last acked (or up to the index where the client didn't want
an ack).  The problem is that the log would need to be iterated over to find
all the notifications for a particular client.  It could get really expensive
if a client goes away for a really long time.

The above also affects how far back the log can be truncated.  That may be ok,
though, since we could always append to a client-specific notification queue as
we truncate the log if the client is too far behind.

The more expensive way is to have that same iterator push notifications onto
persisted client queues always, but then we have to worry about making sure those
queues are correct and unbounded growth.

## Ephemeral state

(Need to think about how we maintain ephemeral state)

## Master should always be changing

We should have the master commit suicide after some amount of time, then the
systems that build on top of vera can be sure they handle the failure mode of
the master going away.  Should probably make a configurable max_leader_ticks
or something (where 0 is never suicide).

## How to combat one misbehaving follower?

Problem:
Because a new term can be initiated by any member that is disconnected from the
leader, there is a possibility of many "false" leader elections, i.e. elections
that didn't need to happen but do because of one bad follower.  For example,
say that there are connectivity problems to one follower.  While it is
disconnected, it keeps timing out its own elections.  Finally, when it is able
to join back, it rejects all AppendEntries requests since its term is way later
than the current master, then the next time it times out it will cause a
"false" leader election.  Chubby's solution to this was to have the master
periodically and artificially boost the term.

That isn't safe in the raft world without another election.  This seems like
the Achilles's heel of raft.

## Server out of configuration disrupting cluster

From the raft paper (section 6, last paragraph):

    The third issue is that servers that are removed from the cluster may still
    disrupt the cluster’s availability.  If these servers do not know that they
    have been removed, they can still start new elections.

They indicate that they are working on a solution.  The naive fix is to have the
raft instance screen requests from servers and reject anything that comes from a
server that isn't part of the latest configuration.  Indeed, vera was already
doing that before I carefully read the cluster reconfiguration section.  If the
incoming requests are filtered to the cluster configuration, any attempt by
an old cluster member to initiate a vote will have no effect.

The issue I see with doing it this way is that there is a possibility that a
server becomes confused and is unable to rejoin a cluster.  Take the
following situation:

1. Three servers, A, B, C.  A is the leader.
2. C goes down.
3. D is added to the cluster.  Since A and B form a majority, this can be
completed.
4. A steps down for some reason and D is elected leader.  Note that A and D
have been attempting to use C this entire time, but it has been offline.
5. C comes back up.
6. D attempts to send requests to C.  C rejects all of them because D is
unknown.

Without doing anything else, C will trigger a new election on timeout.  If the
cluster is lucky, B or A will be elected leader, and C will eventually catch up
with the configuration.  So the cluster will be able to heal, but only if at
least one other member is known to C (not the other way around).  In that case,
manual intervention may be ok.

TODO: Is there some way the cluster can detect an unsafe cluster configuration
change?

## Read-only followers?

Yup, need them.  See the raft section on cluster reconfiguration.

## Bootstrapping a new server coming online as a read-only member

Oct 7 2013 paper suggests that new servers should come online based on a
snapshot from a peer.  That way, it avoids replaying the log and getting
confused about the state of the cluster (reconfigurations cause membership
changes- a new server replaying the entire log could apply a cluster change
that doesn't include the leader that is sending the logs).

So how does a server know that it is "new"?  And how does a raft instance know
that it needs to bootstrap itself from a peer?  How does it locate this peer?
And how does the first raft instance know that it doesn't need to bootstrap
from anything else?

This seems like it should be the same process as adding a read-only peer.  The
first instance should have no peers.  So a new raft instance is bootstrapped
with no data and accepts the first snapshot *or* it is the first peer and it
becomes el jefe.

## Snapshotting

Ok, so here's where abstraction layers totally break down.  Where and how things
are stored are tied to how those things can be efficiently snapshotted and
transmitted.  Even though I want the log, properties, and state machine
persistence to be "independent", the fact is that the most efficient way to
manage those classes of data are in the same place.

There are three sets of persistent things in the raft world:
1. Log
2. State Machine
3. CurrentTerm/VotedFor (not sure if this is necessary to bring over...?)
4. Cluster configuration

The easiest way to do a snapshot and backup is just piping the leveldb directly
over the socket to another leveldb/socket.  Since levelup creates an implicit
snapshot on read, it will probably "just work".  From the leveldb docs, here's
the code to copy a leveldb, from node-land:
```
srcdb.createReadStream().pipe(dstdb.createWriteStream()).on('close', callback)
```

Add some error handling and it should be "fine".  I hope you realize that was
sarcastic.

There are a couple benefits to doing it this way:

1. Creating a backup will compact the db since it won't be taking any of the
deleted data with it (this needs to be tested).
2. Doesn't rely on any os-specific or external-node systems.

Compare that with the "normal" way of taking snapshots, which would also be the
fastest.  Probably.  On SmartOS we'd have the leveldb in a zfs dataset, then we
would take periodic zfs snapshots of the data.  On request, we'd zfs
send/receive.  I'm describing the same thing that manatee does, so we could
steal all the code from there.  Bonus points here is that those zfs snapshots
can be saved offsite for disaster recovery.

So in the short term we'll stream the db data through node and factor it so that
we could have a smartos plugin that would "do the right thing".  We already need
that hook to distinguish between an in-memory raft instance and a leveldb
instance.  In the future, I think we'll only want peers that aren't the master
to flush everything, close leveldb, zfs snapshot, open leveldb and resume.  It
would have to block all upstream traffic to do that... so if some of the peers
did it at the same time write operations could be delayed.  Perhaps the leader
will coordinate it.

With this design, it's necessary that the snapshot sender is paired with a
snapshot receiver since the memory one is going to pass data in memory, the
simple leveldb one is going to pass a serialized stream of data and the smartos
one would be coordinating a zfs send/receive.

Note also that in the raft paper the master decides when a snapshot needs to be
sent and calls out to an api on the follower.  The follower doesn't pull the
snapshot from the leader.  Since zfs send/receive is from server -> client,
should work out.  Worst case scenario is we'd need a PrepareInstallSnapshot
call from leader to follower before the InstallSnapshot.

## LevelDB snapshotting and log truncation.

Keeping the state machine in the same leveldb as the log makes snapshotting and
log truncation dead simple.  Since a leveldb iterator creates an implicit
snapshot, we really don't need a separate snapshot of data- the snapshot is just
the *current state of the db*.  Besides making sure that the log is only ever
truncated up to where the state machine is, the only thing that we *need* to
ensure is that the command log is long enough that:

1) There's enough log that a new instance or a new follower has enough time to
load a snapshot and catch up.  If not, then the new follower will load the
snapshot, then the leader will discover that it is too out of date and the
bootstrapping process will start all over again.
2) There are enough log entries to serve to a lagging follower so that there
isn't a needless bootstrap.  This isn't critical as long as #1 applies.

So.. to sum up:
1) A snapshot isn't anything separate from what the current state of the db is.
2) Vera needs something to figure out what it can delete from the trailing
log... and do it occasionally.

That thing ^^ should write the logs out to flat files on disk that then can be
backed up separately (if desired).

So now just have to figure out how a raft instance can rebootstrap itself from
a snapshot...

## Log Compaction

This needs to be figured out based on vera's use of the log outside raft.

## Election + cluster change === issues?

Is there a way to mess up and have two hosts think they are the leader if a
cluster reconfiguration happens around the time of an election?

No.  Since the cluster changes are part of the normal log committing routines,
if a leader isn't elected, those commands can't be committed to the log.  So
a leader must be stable in order for cluster changes to happen.

## Does LevelDb reclaim the space it uses when deletes are made?  Also, is there
   an efficient way to do range deletes?

http://leveldb.googlecode.com/svn/trunk/doc/impl.html
"Compactions drop overwritten values. They also drop deletion markers if there
are no higher numbered levels that contain a file whose range overlaps the
current key."

No, there is no way to do efficient range deletes.

Good articles:
http://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/
http://lists.basho.com/pipermail/riak-users_lists.basho.com/2012-August/009254.html

## Snapshotting questions...

The snapshotting section of the raft paper leaves some questions.  The first is
what should the bootstrapping raft server do with some of the old values it may
have?  Here is all the persistent state for a raft instance:

1. Current Term
2. Voted For
3. Command log (includes "last log key" in the leveldb implementation)
4. State machine
5. Cluster configuration

Of those, the voted for should definitely *not* transfer to the instance that is
recovering from a snapshot- it should retain the votedFor for that term and
probably shouldn't vote until the current term in the snapshot has been passed.
(in other words, the voted for should be nonsense until the next term change).
It may be OK to set the votedFor to $self.... but need to think through it
more.

The current term should probably transfer directly over... otherwise, how would
the new server know what the current term is?  Perhaps the current term is
populated only with a new append entries?  Yup, it's that last thing.  The node
being bootstrapped should retain its current term and voted for.

Command log should be carried straight over.  I'm not sure why the raft paper
says that the state machine must have the last included index and last included
log for a consistency check.  The snapshot should be carried over unmodified, so
why would it need a consistency check?

I'm also not sure why the authors talk about removing only a portion of the
command log and why they do consistency checks.  If we can assume that the
snapshot is correct, there's no reason to try and muck with the log or the state
machine.  It seems much simpler just to discard the log since the state machine
must be discarded.

## Command at index 0 is the cluster configuration?

As stated elsewhere, raft stores the cluster configuration in the log as
"special" log entries.  Storing the initial configuration at entry 0 serves
several purposes:

1. Serves as the initial consistency check so that there doesn't need to be a
special logic branch for the first entry to arrive.
2. Serves to keep new raft instances *out* of the cluster.  If a new instance
isn't bootstrapped with a configuration, it can't accept append entries
requests.  The only way for a raft instance to come up cleanly is if (1) it is
told it is the first raft instance in a new cluster, (2) it is bootstrapped from
a snapshot (the desireable thing to happen for new cluster members) or (3) an
operator hands it an initial configuration (this seems dangerous, so we want to
make it as hard as possible).
3. Serves as the first, well-known place for the inital cluster configuration.

## What does a machine need to do when it starts up?

When a new machine comes up, it should know only one thing: Whether it is the
first node of a new cluster or not.  If it is, it should get the db in order and
transition to leader.  Otherwise, it starts as a follower and should wait for an
install snapshot or append entries call.  The way the code is currently written,
this new node will know of no cluster configuration and will reject all append
entries requests (since the peers are unknown).

It's clear from the paper that followers make no rpcs on their own.

Questions:
1) How does this new node figure out who it should be accepting initial requests
from?  A: It comes up with no configuration, so just takes the first snapshot
to show up at the door.  We assume that an operator has to add it manually
via a request to the leader.  As long as the new node doesn't have two apply
snapshot requests going on at the same time, things should work out.

## Startup/add sequence

1. startup node A, first === true
2. A transitions to leader, waits for client requests
3. startup node B, first === false
4. client request to A to add B as a read-only member
5. A sends snapshot to B
6. Once B has applied snapshot, it is read-only, ready.
7. A promotes B to a voting member (the old/new process described in the paper)

So perhaps as a deviation from the raft paper, I propose:

1. Snapshots are only sent for bootstrapping new nodes.
2. Operators can recover nodes that are really behind by manually deleting data
(though this should be exceptionally rare), essentially returning it to state
#1.
3. Leaders can truncate the tail of the command log up to the lowest peer match
index.  If implemented correctly, when a new node goes to be bootstrapped, the
leader will naturally wait for that node to catch up, avoiding the problem where
a new follower goes into an infinite recovery loop.

Not sure how the other followers can know what to truncate, though, since they
have no idea what the peer match indexes are... so maybe this won't work even
though it seems simpler than detecting when to send a snapshot based on append
entries.  The raft paper says that the way that the implementation works, a
follower is sent a snapshot if the log entry that the follower needs for append
entries has already been deleted from the command log.

Need to think about this more.

## Where we keep the peers

The raft paper (TODO: add reference) states that the cluster configuration is
modified using special raft entries.  The problem with that in vera was that
log entries are transmitted in streams.  So vera "combs" out those special log
entries in order to work on them.  The raft paper also states (reference?) that
the cluster should use the last configuration in the log.  Perhaps painful, but
this implies that the authoritative source for the latest configuration is in
the command log.

This deviates from the "normal" places cluster configuration is kept (usually in
some special configuration file) and may make it harder to administer a vera
cluster.  But since it is the authoritative place, we'll use it unless it
actually does present an administrative challenge.

So this explains why the command log has the peer interfaces.

## Truncating a configuration?!

Since the authoritative source of the cluster configuration is the last cluster
configuration log entry *whether it is committed or not*, there's the
possibility that:

1. A follower gets an updated config (Cold+new), starts using it
2. The leader fails
3. A new leader gets elected that doesn't have Cold+new.
4. New leader accepts a command, meaning the Cold+new is invalid.
5. New leader causes log on follower to truncate behind the cluster
   configuration location.

What should a follower do when the log is truncated?  Note that there is a
two phase approach to cluster reconfigs, first Cold+new is *committed*, meaning
that all servers will eventually get that configuration before Cnew is
propagated.  That means that if a server starts using Cold+new, the only time
it would ever go back to Cold is if the log is truncated behind that log entry.
Also keep in mind that vera's raft will reject a second cluster reconfiguration
attempt as long as one is in progress (The last C has to be committed before
a new configuration request can be made).

What does this mean for the follower in our example above?  We can roll back as
far as we need to go by keeping a back-pointer to the last config entry.  That
way, as we truncate, we can load up the right "last" config.  For example, this
is what would be logically written:

1. Start with 0->{ current } (current configuration is at index 0).
2. Request comes in to update the config, now we persist 5->{ new, old: 0 }
3. On truncation back before 5, we persist 0->{ current }
4. On committal of 5->{ new, old: 0 }, we're good for the moment.
5. Now we go to persist 10->{ current, old: 5 }
6. On truncation back before 10, we persist 5->{ new, old: 0 }
7. On committal of 10->{ current, old: 5 }, we're all done with the config
   changes.

In the leveldb, the simple thing we'll do is keep an internal property, the
`clusterConfigPointer` (so we're really only persisting 0, 5, 10, not the full
configuration).

## Problems when leader isn't part of Cnew when clients check access list?

Walking through the following scenario:

1. Leader receives request to take itself out of the cluster
2. Leader sends Cold,Cnew entry (via append entries) to peers in Cold,new.
3. Followers accept, persist and update cluster with Cold,new.  Return +1.
4. Leader accepts majority (majority of Cold,new)
5. Leader sends Cnew entry (via append entries) to peers in Cnew.
6. Follower accept, persist, and update cluster with Cnew.  At this point, the
   followers won't accept any more append entries from the leader because
   !self.cluster.peerExists(req.leaderId
7. Leader accepts majority (majority of Cnew)
8. Leader steps down because it isn't part of Cnew.

So, no, things should work fine if the clients still check their access list.

## What do peers that aren't in Cnew do with themselves?

Raft nodes can be leaders, candidates and followers.  The raft paper also
suggests that followers can be read-only.  It doesn't address what happens when
a follower is removed from the cluster configuration or what it actually means
to be a read-only follower except "the leader will replicate log entries to
them, but they are not considered for majorities" (section 6).

Followers don't need to know where they are in a cluster (or what they are in a
cluster) until they time out waiting for the leader to send append entries or
receive a request vote.  If they receive a request vote and they think they
aren't a voting member, what should they do?  Are they confused, or does the
remote side know more than they do?

What is a case where the follower is confused?

Leader sends Cold,new to A.  In Cnew B is a voting member.  Leader crashes
before it sends Cold,new to B.  A times out and requests a vote from B.

What is a case where the requester is confused?

Follower has config Cold where B is a voting member.  Follower crashes.  Cnew
propagates completely, which removes B from being a voting member.  Follower
comes back up, times out and requests that B votes.

Ok, so now we know there are possibilities on either side, what is the correct
behavior for request vote?

Can we take the safest approach of rejecting when a follower believes it is not
voting?  So if a follower is in read-only it will:

1. reject requestVote requests
2. on timeout, just reset the ticker (don't transition to candidate)

And how should a follower act when it is a Cold,new and old,new don't agree if
the follower is read-only or not?

If we assume that only one cluster config change can go in at a time (since
that is how the code is currently written)?

Can only give a vote to a peer that is part of a configuration where the
follower is a voting member seems the safest.

Note: Only one change can propagate at a time, so this is invalid since C is
      demoted and D is promoted at the same time.

- AllPeers:   A, B, C, D
- ColdVoters: A, B, C
- CnewVoters: A, B, D

Examples:

#### Demoting A

- AllPeers:   A
- ColdVoters: A
- CnewVoters:

Is this ever a valid configuration? Could be used to put a single-node cluster
in read-only mode as part of retiring a cluster.  But coming back out would be
impossible since there'd be no way of bringing it back into voting status other
than hacking data.  For now, it should be an error to have one instance with
non-voting status in a configuration.  TODO: Figure out if there is ever a case
where we want this.  Seems like it'd be easier just to just have an on/off
switch on the leader for accepting new client requests from the outside.

#### Promoting B

- AllPeers:   A, B
- ColdVoters: A
- CnewVoters: A, B

Only A can be elected leader.  If A goes down when this is the current config
should B be able to finish the reconfiguration?  According to the raft paper,
no.  That is the correct thing since we can't be sure that B has received all of
A's committed entries before A crashed.

B shouldn't ever transition to candidate.  A must be up for the cluster to make
progress.

#### Demoting B

- AllPeers:   A, B
- ColdVoters: A, B
- CnewVoters: A

Want to avoid electing B.  A should be able to elect itself if B goes down.
A shouldn't accept votes from B.  This seems like a situation that would happen
if B were to go down, become irrecoverable, and an operator wants to remove B
from the cluster.  If A goes down, the cluster cannot make progress.

Wait... what if B was leader?  A has to have all of B's *committed* entries (and
perhaps more) because B couldn't have been making progress without A.  A could
take leadership at any time without adverse effects.

#### Demoting C

- AllPeers:   A, B, C
- ColdVoters: A, B, C
- CnewVoters: A, B

Want to avoid electing C.  A or B should be able to be elected leader.  A and
B must be up to make progress, and they are the only ones that matter.

#### Promoting C

- AllPeers:   A, B, C
- ColdVoters: A, B
- CnewVoters: A, B, C

Only A or B can be elected leader since only they are guarenteed to have all the
log entries.  Followers should never transition to candidate unless they are
voting member in both Cold and Cnew.

What about A completes the config change to B, but not to C, then A crashes?

* C can't be ahead of B or else it would have the config change that B has.
* B can't make progress until C is up to date.
* B is will have everything that A had committed since A couldn't have made
  progress without it.

Seems that it is safe for C to cast the vote for C.

#### Adding C

- AllPeers:   A, B, C
- ColdVoters: A, B
- CnewVoters: A, B

A and B are the only ones that matter.

#### Promoting E

- AllPeers:   A, B, C, D, E
- ColdVoters: A, B, C, D
- CnewVoters: A, B, C, D, E

There's plenty of redundency here that A, B, C, and D can make progress if any
one of them goes down.  E shouldn't be elected because it isn't guarenteed to
have all

#### Demoting E

- AllPeers:   A, B, C, D, E
- ColdVoters: A, B, C, D, E
- CnewVoters: A, B, C, D

E shouldn't be elected leader since it is a waste to elect it.

#### Given the above:

* Append entries should be accepted as long at the sender is in the list of
  peers.
* Followers should max their ticker on every tick if they are read-only in any
  of Ccurrent, Cold, Cnew since they can't be elected in any of those
  configurations.
* Followers should only vote for a candidate if the candidate is a voting member
  in *all* configs.
* Followers should only cast votes for candidates if it is a voter in Cnew or
  Ccurrent.
