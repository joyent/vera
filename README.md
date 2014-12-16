<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

vera
====

Vera is a consensus service based on the Raft consensus algorithm.  **It is
currently a work in progress and is not complete**.  See the docs directory for
design:

* [Vera TODOs](docs/vera_todo.txt)
* [Interface](docs/interface.md) The intended Vera interface.
* [Schema](docs/schema.md) The LevelDB schema.
* [Design Rationale](vera/design_rationale.md) A brain dump of why Vera is the
  way it is.  WARNING: it is a brain dump, so may not be totally coherent.
* [Use Cases](docs/use_cases.md)
* [References](docs/references.md) for Raft, etc.

# Running the REPL

Vera comes with a Raft repl that uses an entirely in-memory raft cluster for
testing.  This is the same repl used for some of the raft tests.  Here is a
short tutorial:

```
vera$ ./bin/repl.js
# Create an initial cluster
> cluster

# Print out the current state
> print
# Rafts:
raft-0  follower term:   0, t-out:  5, leader: undefd, commitIdx: 0
raft-1  follower term:   0, t-out: 10, leader: undefd, commitIdx: 0
raft-2  follower term:   0, t-out: 10, leader: undefd, commitIdx: 0
# Messages:
Messages: (none)

# "Tick" the cluster forward and see what t-outs have decreased
> tick
> print
# Rafts:
raft-0  follower term:   0, t-out:  4, leader: undefd, commitIdx: 0
raft-1  follower term:   0, t-out:  9, leader: undefd, commitIdx: 0
raft-2  follower term:   0, t-out:  9, leader: undefd, commitIdx: 0
# Messages:
Messages: (none)

# You can ls and print the state of anything
> ls
log
nextRaft
raftIds
messageBus
console
batch
cluster
raft-0
raft-1
raft-2
> get raft-1.state
follower

# Make raft-0 ready to become a leader, tick the cluster until it is
> set raft-0.leaderTimeout 0
> tick cluster until equal raft-1.leaderId "raft-0"
> tick cluster until equal raft-2.leaderId "raft-0"
> print
# Rafts:
raft-0    leader term:   1, t-out:  4, leader: raft-0, commitIdx: 0
raft-1  follower term:   1, t-out:  6, leader: raft-0, commitIdx: 0
raft-2  follower term:   1, t-out:  8, leader: raft-0, commitIdx: 0
# Messages:
Messages: (none)

# Make a client request to set the state machine data to foo
> request leader "foo"
> print
# Rafts:
raft-0    leader term:   1, t-out:  4, leader: raft-0, commitIdx: 0
raft-1  follower term:   1, t-out:  6, leader: raft-0, commitIdx: 0
raft-2  follower term:   1, t-out:  8, leader: raft-0, commitIdx: 0
# Messages:
Messages:
  appEntr raft-0 -> raft-1: term   1, leader: raft-0, commitIndex:   0
  appEntr raft-0 -> raft-2: term   1, leader: raft-0, commitIndex:   0
> tick cluster until equal raft-2.stateMachine.data "foo"
{ leaderId: 'raft-0', entryTerm: 1, entryIndex: 1, success: true }
> print
# Rafts:
raft-0    leader term:   1, t-out:  4, leader: raft-0, commitIdx: 1
raft-1  follower term:   1, t-out:  8, leader: raft-0, commitIdx: 1
raft-2  follower term:   1, t-out:  9, leader: raft-0, commitIdx: 1
# Messages:
Messages: (none)
> get raft-0.stateMachine.data
foo

# Partition off the leader and see that another one is elected
> partition raft-0
> tick cluster 20
> print
# Rafts:
raft-0    leader term:   1, t-out: -17, leader: raft-0, commitIdx: 1
raft-1  follower term:   3, t-out:  9, leader: raft-2, commitIdx: 1
raft-2    leader term:   3, t-out:  4, leader: raft-2, commitIdx: 1
# Messages:
Messages: (none)

# Finally, unpartition and see old leader joins back as follower
> unpartition raft-0
> tick cluster 15
> print
# Rafts:
raft-0  follower term:   3, t-out:  5, leader: raft-2, commitIdx: 1
raft-1  follower term:   3, t-out:  2, leader: raft-2, commitIdx: 1
raft-2    leader term:   3, t-out:  0, leader: raft-2, commitIdx: 1
# Messages:
Messages:
  appEntr raft-2 -> raft-0: term   3, leader: raft-2, commitIndex:   1
  appEntr raft-2 -> raft-1: term   3, leader: raft-2, commitIndex:   1

```

You can also put a list of commands in a file and rnu them:
```
vera$ ./bin/repl.js -f test/repl_tests/raft_init/no_cluster_config.txt
> raft
raft-0
> tick raft-0 2
> assert equal raft-0.cluster.clogIndex -1
> assert equal raft-0.leaderTimeout 10
```

See the [test/repl_tests](test/repl_tests) directory for more command examples.

# Running tests

```
$ npm install
$ npm test
```
