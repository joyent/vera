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

Next TODO...
