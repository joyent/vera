## Schema Intro

This describes how the LevelDB is laid out.  For some backgroud, a LevelDB is a
`key -> value` stor a la Berkley DB, Redis, etc.  It stores data on disk.  As
with all "NoSQL" stores, namespaces, indexes, etc. are all "manual".  It may be
helpful to know some features/limitations of LevelDB:

* Keys and values are arbitrary bytes (the Node levelup library puts some
  suger on top of that)
* Data is stored in sorted order, so iteration in sorted order is "cheap".
* Data can be put in batches.  These batches are transactional, meaning the
  put succeeds or fails for all elements within the batch.
* Iterators are implemented as if they take an implicit snapshot.  This means
  consistent reads during iteration as long as data is put in "transactional
  batches".
* Only one process can access a LevelDB at a time.

From Node's (levelup's) perspective, all data in the LevelDB is stored as keys
of type `Buffer` and values as `JSON`.

## Raft Layout

All data is partitioned into buckets:

| Byte Prefix | Name | Notes |
| --- | --- | --- |
| `0x0000` | String properties | Properties that are specific to an individual Vera peer.  For example, the `votedFor` property. |
| `0x0001` | Internal String properties | Properties that could be shared across all Vera peers, specific to *this* db.  For example, the `lastLogIndex`, which is a property of the Command Log. ` |
| `0x0002` | Command Log entries | This is the Raft Command Log. |
| `0x0003` | State machine entries | This is where all the data is kept once the entry has passed via the Command Log. |

More details are given in the sections below.  Note that because all instance
specific data is at prefix > `0x0001`, snapshots in Vera are taken by starting
an iterator at `0x0001` and reading until the end of the DB.  Snapshots are
applied by writing over the current state of the db (keeping the
instance-specific data intact).

### String Properties

After the prefix `0x0000`, the keys are utf-8 byte strings.  For example:

```
nate.local: vera(master) node $> var key = require('./lib/leveldb/key');
undefined
nate.local: vera(master) node $> key.property('foo');
<Buffer 00 00 66 6f 6f>
nate.local: vera(master) node $> (new Buffer('666f6f', 'hex')).toString('utf-8')
'foo'
```

### Internal String Properties

These are encoded in the same way that `String properties` are, but with a
prefix of `0x0001`:

```
nate.local: vera(master) node $> key.internalProperty('foo');
<Buffer 00 01 66 6f 6f>
```

### Command Log entries

After the prefix `0x0002`, each command log entry has an integer index.  See the
Raft paper for details on how this is used in Raft.  For Vera's purposes, we
simply encode the integer into an 8-byte buffer.  For example:

```
nate.local: vera(master) node $> key.log(1);
<Buffer 00 02 00 00 00 00 00 00 00 01>
nate.local: vera(master) node $> key.log(255);
<Buffer 00 02 00 00 00 00 00 00 00 ff>
nate.local: vera(master) node $> key.log(9007199254740991);
<Buffer 00 02 00 1f ff ff ff ff ff ff>
```

### State machine entries

This is left up to the state machine implementation.  For Vera, see the next
section.  At the time of this writing, the data partition wasn't used in the
state machine for Raft testing.  An internal property was used instead.

## Vera Data Layout

Vera's interface needs to support the following entities:

* List Items
* Clients
* Implicit Watches
* Explicit Watches

The lookups we need to support:

| From | To | API / Function |
| --- | --- | --- |
| Client | Ephemeral data client holds | Expiration of data. |
| Watch | Set of clients | Send out events. |

### Schema

| BytePrefix | Type | Key | Value |
| --- | --- | --- |
| `0x0003` | List Item | `/data/path/to/list` + `0x0000` + `raftIndex` | `{ data:, clientId: }` |
| `0x0004` | Client | `/client/` + `clientId` | `{ remoteIp:, timeout: }` |
| `0x0005` | Client Data | `/cdata/` + `clientId` + `0x0000` + `[List Item Key]` | `` |
| `0x0006` | Watch | `/watch/path/to/list` + `0x0000` + `clientId` | ` ` |
| `0x0007` | Client Lag | `/lag/` + `clientId` | `{ index: }` |

_TODO: Spread out the above a litte more?_

### Scratch

* Where to keep the client notification position?  Do we even need one?

Goals:
1) Only write data for the client on needs notification.
2) Keep a minimal amount of data for the client, update data as little as
   possible.

On server:
1) Last client heartbeat

In Raft:

New leaders need to set their own client timeouts for reconnect.  Shouldn't this
be the same

Notifications
Ephemeral Data

### Ephemeral data walkthrough:

Ephemeral data walkthrough (substituting `|` for `0x0000`):
1. Client A connects:
    * Raft write:
        * `/client/A -> { 'remoteIp': '127.0.0.1', 'timeout': 30 }`
    * In memory:
        * `self.expirationQueue.push('A', 30);`
        * `self.connections['A'] = websocket`
1. Client A pushes "x" on list /foo/bar
    * Raft write:
        * `/data/foo/bar|0001 -> { 'data': 'x', 'clientId': 'A' }`
        * `/cdata/A|/data/foo/bar|0001`
        * `/watch/foo/bar|A`
1. Client heartbeat
    * In memory:
        * `self.expirationQueue.reset('A')`
1. Client fails to heartbeat
    * In memory:
        * `self.expirationQueue.emit('expired', { 'id': 'A' })`
    * Raft lookup:
        * `list '/cdata/A|'`
    * Raft write (this would be done in a batch, not one per):
        * `delete '/cdata/A|/data/foo/bar|0001'`
1. Leader fails, new leader:
    * List Clients
        * `list /client`
    * Set expirations (in memory):
        * `new self.expirationQueue`
        * `foreach client {self.expirationQueue.push(c.id, c.timeout); }`

From the above, Raft writes only occur on:
1. Client Connects
2. Data writing
3. Data expiration

### Notification Walkthrough

1. Leader, inits notifier:
    * Raft read:
        * `internalProperty('notificationIndex') -> null`
    * In memory:
        * `new Notifier(0000)`
1. Client A connects (see above)
1. Client B connects (see above)
1. Client B watches /foo/bar:
    * Raft write:
        * `/watch/foo/bar|B`
1. Client A pushes "x" on list /foo/bar
    * Raft write:
        * `/data/foo/bar|0001 -> { 'data': 'x', 'clientId': 'A' }`
        * `/cdata/A|/data/foo/bar|0001`
        * `/watch/foo/bar|A`
    * On complete
        * `stateMachine.emit('dataChange', { 'type': 'add', ...} )`
1. Notifier recieves event
    * _NOTE: This might be the place where reading from a leveldb snapshot is
      the thing to do for consistent reads.  In this case, the snapshot would be
      taken before the stateMachine emits and sent along with the event._
    * Find clients that are interested in this list:
        * `list /watch/foo/bar|'
    * For each client, send notifications:
        * `self.connections['A'].sendNotification(...)`
    * Raft write (all in batch):
        * Give each client a "reasonable time" to respond.  For each client that
          doesn't: `/lag/A -> { 0000 }`
        * `internalProperty('notificationIndex') -> 0000`

How do clients catch up?
When is it appropriate to write client data?
It she same problem for appends as it is for client notifications, isn't it?
  Is there any way to use the same mecahnism?

Client connects, needs notifications.
Maintain an internal client notification queue?

Options:
Central thing that controls notifications (what is above)
Notifiers spawned as needed for a client when it has outstanding notifications.
Internal producer/consumer for notifiers.
What is going to be as timely as possible and the simplest to understand?

Requirements:
1) Clients are notified as quickly as possible.  Notifications should be tied to
   data changes, not to client heartbeats.
2) Don't require raft writes on each client heartbeat.  Don't tie how many times
   we write to clients with the number of writes to Raft.
3)

A raft that takes over has to know where clients are.  Data must exist in Raft
via the "normal" raft mechanisms (there's no reason to make some other
data replication system).

Keep a pointer for each client.

Keep the lowest notification for each client that's lagging behind.

Keep a list of elements for a client when it is lagging behind.

Is it that we want to reduce the amout of data in the leveldb?  Or just not have
a bunch of client cruft in there?  If we do log compaction, how much does having
cruft client data really matter?
