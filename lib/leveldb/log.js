// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var error = require('../error');
var events = require('events');
var lib = require('../leveldb');
var memStream = require('../../lib').memStream;
var PairsStream = require('../pairs_stream');
var Readable = require('stream').Readable;
var sprintf = require('extsprintf').sprintf;
var stream = require('stream');
var util = require('util');



/**
 * LevelDB (https://code.google.com/p/leveldb/) used as the backing persistance
 * layer for the raft log.
 */

///--- Globals

var propertyKey = lib.internalPropertyKey;
var logKey = lib.logKey;
var errToObj = lib.errToObj;
var SYNC_OPTIONS = { 'sync': true };
var LAST_INDEX_KEY = propertyKey('lastLogIndex');



///--- Functions

function LevelDbLog(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.optionalString(opts.location, 'opts.location');
    assert.optionalObject(opts.db, 'opts.db');
    assert.object(opts.stateMachine, 'opts.stateMachine');

    var self = this;
    self.log = opts.log;
    self.stateMachine = opts.stateMachine;
    self.open = false;
    openDb.call(self, opts);
}

util.inherits(LevelDbLog, events.EventEmitter);
module.exports = LevelDbLog;


///--- Helpers

/**
 * Does the actual initing of the leveldb log.  It will emit either ready or
 * error exactly once.
 */
function openDb(opts) {
    var self = this;
    var log = self.log;
    var lastIndex;

    function setupSelf() {
        log.debug({ 'index': lastIndex }, 'reading last entry');
        opts.db.get(logKey(lastIndex), function (err, l) {
            if (err || !l) {
                log.fatal({ 'error': errToObj(err), 'last': l },
                          'Error fetching last entry in log');
                return (self.emit('error', err));
            }
            self.nextIndex = l.index + 1;
            self.lastEntry = l;
            self.db = opts.db;
            self.open = true;
            return (self.emit('ready'));
        });
    }

    function initDb() {
        //The Raft paper says that the index should start at one.  Rather
        // than doing that, a fixed [0] ensures that the consistency check
        // will always succeed.
        var firstLogEntry = { 'term': 0, 'index': 0, 'command': 'noop' };
        opts.db.batch([
            { 'type': 'put',
              'key': logKey(0),
              'value': firstLogEntry
            },
            { 'type': 'put',
              'key': LAST_INDEX_KEY,
              'value': 0
            }
        ], SYNC_OPTIONS, function (err) {
            if (err) {
                log.fatal({ 'error': errToObj(err) },
                          'Error writing first log entry');
                return (self.emit('error', err));
            }
            lastIndex = 0;
            setupSelf();
        });
    }

    function findLastIndex() {
        opts.db.get(LAST_INDEX_KEY, function (err, value) {
            if (err && err.name === 'NotFoundError') {
                return (initDb());
            }
            if (err) {
                log.fatal({ 'error': errToObj(err) },
                          'Error finding index in db');
                return (self.emit('error', err));
            }
            lastIndex = value;
            setupSelf();
        });
    }

    //If there's an open db passed in, then skip opening it ourselves.
    if (opts.db !== null && opts.db !== undefined) {
        return (findLastIndex());
    }

    log.debug({ 'opts.location': opts.location },
              'opening/initing leveldb log');
    lib.createOrOpen(opts, function (err, res) {
        if (err) {
            return (self.emit('error', err));
        }

        opts.db = res.db;
        findLastIndex();
    });
}



///--- Internal classes

function ValueTransform(a) {
    stream.Transform.call(this, { 'objectMode': true });
}
util.inherits(ValueTransform, stream.Transform);

ValueTransform.prototype._transform = function (data, encoding, cb) {
    if ((typeof (data)) === 'object') {
        if (data.value !== undefined) {
            data = data.value;
        }
    }
    this.push(data);
    cb();
};



///--- API

//TODO: A better way?
LevelDbLog.prototype.from = function from(opts, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.db, 'opts.db');
    assert.object(opts.stateMachine, 'opts.stateMachine');

    var self = this;
    var l = new LevelDbLog({
        'log': self.log,
        'db': opts.db,
        'stateMachine': opts.stateMachine
    });
    l.on('ready', function () {
        return (cb(null, l));
    });
    l.on('error', function (err) {
        return (cb(err));
    });
};


LevelDbLog.prototype.append = function append(opts, cb) {
    assert.object(opts, 'opts');
    assert.number(opts.commitIndex, 'opts.commitIndex');
    assert.number(opts.term, 'opts.term');
    assert.object(opts.entries, 'opts.entries');

    var self = this;
    var log = self.log;
    var db = self.db;
    var commitIndex = opts.commitIndex;
    var term = opts.term;
    var entries = opts.entries;
    var firstEntry = null;
    var level = null;
    var ended = false;
    var entriesEnded = false;
    var psEnded = false;
    var lastEntry = null;

    log.debug('append start');
    if (self.open === false) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('Attempt to use leveldb_log before ' +
                                          'fully initialized'))));
    }

    function end(err, entry) {
        log.debug('something called end');
        if (ended) {
            if (err) {
                log.error(err, 'err after already ended');
            }
            return;
        }

        if (level !== null) {
            log.debug('removing all level listeners');
            level.removeAllListeners();
            //Note that this destroy is on the iterator *not the db*.
            level.destroy();
        }
        log.debug('removing all entries listeners');
        entries.removeAllListeners();
        ended = true;

        //Final sanity checking

        //Checking to see that the final entry index is at or more than the
        // commit index of the request.  If it's not, we have to error.
        // Otherwise, the leader will set the commit index for this node ahead
        // of where it actually is (that's a bad thing).
        log.debug({ err: err, entriesEnded: entriesEnded, psEnded: psEnded,
                    commitIndex: commitIndex,
                    lastEntry: lastEntry }, 'checking final');
        if (!err && (entriesEnded || psEnded) &&
            lastEntry !== null && commitIndex > lastEntry.index) {
            log.debug('last entry index is behind the commit index');
            err = new error.InvalidIndexError(sprintf(
                'commit index %d is ahead of the entry index %d',
                commitIndex, lastEntry.index));
        }

        log.debug('append end');
        return (cb(err, entry));
    }

    function onTransitionalEntryEnd() {
        entriesEnded = true;
    }

    function onFirstLevelEnd() {
        //This means that the entry isn't in the log yet, so we close
        // everything and fail out.
        log.debug('ending due to first level end');
        return (end(new error.TermMismatchError(sprintf(
            'no clog entry at index %d', firstEntry.index))));
    }

    function onFirstLevelError(err) {
        log.error(errToObj(err), 'on first level error');
        return (end(err));
    }

    function onFirstLevelReadable() {
        var firstLevel = level.read();
        if (firstLevel === null) {
            level.once('readable', onFirstLevelReadable);
            return;
        }
        log.debug('first level read');
        entries.removeListener('end', onTransitionalEntryEnd);
        level.removeListener('error', onFirstLevelError);
        level.removeListener('end', onFirstLevelEnd);
        firstLevel = firstLevel.value;

        //Now we have the first entry from both streams and we can do the
        // consistency check.
        if (firstLevel.index !== firstEntry.index ||
            firstLevel.term !== firstEntry.term) {
            log.debug({
                'levelIndex': firstLevel.index,
                'levelTerm': firstLevel.term,
                'entryIndex': firstEntry.index,
                'entryTerm': firstEntry.term
            }, 'ending due to term mismatch');
            return (end(new error.TermMismatchError(sprintf(
                'at entry %d, command log term %d doesn\'t match %d',
                firstEntry.index, firstLevel.term, firstEntry.term))));
        }

        //If we caught an end event for the entries while we were waiting for
        // reading from the leveldb, we can just end here.
        if (entriesEnded) {
            log.debug('ending due to transitional entries ending');
            return (end());
        }

        var ps = new PairsStream({ 'left': entries, 'right': level });

        var outstandingPuts = 0;
        var successfulPuts = 0;
        var trackingIndex = firstEntry.index + 1;
        var trackingTerm = firstEntry.term;
        var truncated = false;
        var stateMachine = self.stateMachine;

        function tryEnd() {
            log.debug({  'outstandingPuts': outstandingPuts,
                         'successfulPuts': successfulPuts,
                         'psEnded': psEnded
                      }, 'trying end');
            if (!psEnded || (outstandingPuts !== successfulPuts)) {
                return;
            }
            log.debug({ 'outstandingPuts': outstandingPuts,
                        'successfulPuts': successfulPuts },
                      'ending because the pairs stream has ended and ' +
                      'all entries have been flushed.');
            return (end());
        }

        function forcePsEnd(err) {
            log.debug('removing all ps listeners');
            ps.removeAllListeners();
            return (end(err));
        }

        ps.on('readable', function () {
            var pair;
            while (null !== (pair = ps.read())) {
                log.debug({ 'pair': pair }, 'pair read');
                var e = pair.left;
                //Walk down to the value for the level db record
                var l = (pair.right === undefined || pair.right === null) ?
                    pair.right : pair.right.value;

                //The entries ending means we can just stop, but after all the
                // writes are flushed.
                if (e === undefined || e === null) {
                    //We lie here because, for all intents and purposes, we
                    // don't care to read the rest of the ps stream.  The
                    // cleanup for the two streams are done elsewhere.
                    log.debug('removing all ps listeners, fake end');
                    ps.removeAllListeners();
                    psEnded = true;
                    return (tryEnd());
                }
                lastEntry = e;

                //Verify that the indexes are strictly increasing
                if (e.index !== trackingIndex) {
                    log.debug('ending because entry index isnt strictly ' +
                              'increasing.');
                    return (forcePsEnd(new error.InvalidIndexError(sprintf(
                        'entry index isn\'t strictly increasing at %d',
                        e.index))));
                }
                ++trackingIndex;

                //And the term is increasing
                if (e.term < trackingTerm) {
                    log.debug('ending because entry term is less than ' +
                              'tracking term.');
                    return (forcePsEnd(new error.InvalidTermError(sprintf(
                        'term %d isn\'t strictly increasing at index %d',
                        e.term, e.index))));
                }
                trackingTerm = e.term;

                //Verify that the entries don't have a term past raft's term
                if (term < e.term) {
                    log.debug('ending because term is less than entry term');
                    return (forcePsEnd(new error.InvalidTermError(sprintf(
                        'request term %d is behind the entry term %d',
                        term, e.term))));
                }

                if (l !== null && l !== undefined && !truncated) {
                    //Sanity checking...
                    if (e.index !== l.index) {
                        log.debug('ending because indexes arent equal');
                        return (forcePsEnd(new error.InvalidIndexError(sprintf(
                            'entry index %d doesn\'t equal db index %d in ' +
                                'pairs stream', e.index, l.index))));
                    }

                    //Truncate if we need to.  By setting this to true, the
                    // entries will be written *including the index*,
                    // effectively truncating the log.
                    if (e.term !== l.term) {
                        //Up until now, all the records should have been
                        // read-and-verify only.  Since we're at the point where
                        // we'll actually do damage (truncation), we do some
                        // sanity checking.
                        if (stateMachine.commitIndex >= e.index) {
                            var message = sprintf(
                                'attempt to truncate before state machine\'s ' +
                                    'commit index', e.index);
                            log.error({
                                'stateMachineIndex': stateMachine.commitIndex,
                                'oldEntry': l,
                                'newEntry': e
                            }, message);
                            return (forcePsEnd(
                                new error.InternalError(message)));
                        }
                        log.debug('truncating');
                        truncated = true;
                        //If we don't do this here, we won't correctly set the
                        // internal state correctly.
                        self.nextIndex = e.index + 1;
                        self.lastEntry = e;
                    }
                }

                //If the leveldb goes null, that means we can blast the rest
                // of the pairs stream into leveldb.
                //TODO: Should we just to one big batch at the end or several
                // smaller batches?  All the little fsyncs are going to kill us.
                // Or maybe we can just fsync at the very end?  Also, putting
                // the index each time sucks.  Doing a real batch here is *much*
                // smarter.  We're going to have to perf-test this and see what
                // the right thing to do is.
                if (l === undefined || l === null || truncated) {
                    ++outstandingPuts;
                    log.debug({ 'entry': e }, 'putting entry');
                    db.batch([
                        { 'type': 'put',
                          'key': logKey(e.index),
                          'value': e
                        },
                        { 'type': 'put',
                          'key': LAST_INDEX_KEY,
                          'value': e.index
                        }
                    ], SYNC_OPTIONS, function (err) {
                        log.debug({ 'entry': e }, 'entry put done');
                        if (err) {
                            log.error({ 'error': errToObj(err), 'entry': e },
                                        'error putting entry, forcing end.');
                            return (forcePsEnd(err));
                        }
                        //Only ever increasing...
                        if (e.index > self.lastEntry.index) {
                            self.nextIndex = e.index + 1;
                            self.lastEntry = e;
                        }
                        ++successfulPuts;
                        tryEnd();
                    });
                }
            }
        });

        ps.on('end', function () {
            psEnded = true;
            tryEnd();
        });
    }

    function onFirstEntryEnd() {
        if (firstEntry !== null) {
            //The onFirstEntryReadable should take care of this.
            return;
        }
        log.debug('ending on first entry end, null');
        entriesEnded = true;
        return (end());
    }

    function onFirstEntryError(err) {
        log.error(errToObj(err), 'on first entry error');
        return (end(err));
    }

    function onFirstEntryReadable() {
        firstEntry = entries.read();
        if (firstEntry === null) {
            entries.once('readable', onFirstEntryReadable);
            return;
        }
        log.debug('first entry read');
        lastEntry = firstEntry;
        entries.removeListener('error', onFirstEntryError);
        entries.removeListener('end', onFirstEntryEnd);

        //Sanity check.  We'll blow up if the term < 0
        if (firstEntry.index < 0) {
            var tmerr = new error.TermMismatchError(sprintf(
                'at entry %d, term %d is invalid', firstEntry.index,
                firstEntry.term));
            return (end(tmerr));
        }

        //A single put.
        if (firstEntry.index === undefined) {
            //TODO: Check that the entry has all the right fields
            firstEntry.index = self.nextIndex;
            ++self.nextIndex;
            //TODO: If this fails, we're going to get holes in the log.
            db.put(logKey(firstEntry.index), firstEntry,
                   SYNC_OPTIONS, function (err) {
                       if (firstEntry.index > self.lastEntry.index) {
                           self.lastEntry = firstEntry;
                       }
                       log.debug('ending on single put');
                       return (end(err, firstEntry));
                   });
            return;
        }

        //Otherwise, we start reading from a leveldb iterator.
        log.debug('starting read from leveldb');
        level = db.createReadStream({
            'start': logKey(firstEntry.index),
            'end': logKey(self.nextIndex - 1)
        });

        level.once('readable', onFirstLevelReadable);
        level.once('error', onFirstLevelError);
        level.once('end', onFirstLevelEnd);
        //Adding this transitional listener guarentees we'll see the end
        // event for entries for as long as it takes to read from the leveldb.
        entries.on('end', onTransitionalEntryEnd);
    }

    log.debug('starting read from entries');
    entries.once('readable', onFirstEntryReadable);
    entries.once('error', onFirstEntryError);
    entries.once('end', onFirstEntryEnd);
};


LevelDbLog.prototype.slice = function slice(start, end, cb) {
    assert.number(start, 'index');
    if ((typeof (end)) === 'function') {
        cb = end;
        end = undefined;
    }
    assert.optionalNumber(end, 'index');
    assert.func(cb, 'cb');
    var self = this;
    var log = self.log;
    if (self.open === false) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('Attempt to use leveldb_log before ' +
                                          'fully initialized'))));
    }

    //Make sure we only go up to the end- the log itself could go further if
    // there was a previous truncation.
    if (end === undefined) {
        end = self.lastEntry.index;
    } else {
        //We subtract 1 here to make this function act like javascript's slice.
        end = Math.min(end - 1, self.lastEntry.index);
    }

    //Just nip it if there's nothing to slice.
    if (end < start) {
        return (setImmediate(cb.bind(null, null, memStream([]))));
    }

    log.debug({ 'start': start, 'end': end }, 'slicing');
    var rs = self.db.createReadStream({
        'start': logKey(start),
        'end': logKey(end)
    });
    var vt = new ValueTransform();
    rs.pipe(vt);
    setImmediate(cb.bind(null, null, vt));
};


LevelDbLog.prototype.last = function last() {
    var self = this;
    return (self.lastEntry);
};


LevelDbLog.prototype.close = function close(cb) {
    cb = cb || function () {};
    var self = this;
    if (self.open === false) {
        return (setImmediate(cb));
    }
    if (self.db.isOpen()) {
        self.db.close(function () {
            self.open = false;
            return (cb());
        });
    }
};


LevelDbLog.prototype.dump = function dump(cb) {
    cb = cb || function () {};
    var self = this;
    var db = self.db;
    db.createReadStream()
        .on('data', function (d) {
            console.log(JSON.stringify(d, null, 0));
        }).on('close', cb);
    return;
};
