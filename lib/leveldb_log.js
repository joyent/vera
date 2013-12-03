// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var error = require('./error');
var events = require('events');
var levelup = require('level');
var PairsStream = require('./pairs_stream');
var Readable = require('stream').Readable;
var sprintf = require('extsprintf').sprintf;
var util = require('util');



/**
 * LevelDB (https://code.google.com/p/leveldb/) used as the backing persistance
 * layer for the raft log.
 */

///--- Functions

function LevelDbLog(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.location, 'opts.location');
    assert.object(opts.stateMachine, 'opts.stateMachine');

    var self = this;
    self.log = opts.log;
    self.stateMachine = opts.stateMachine;
    self.open = false;
    initDb.call(self, opts);
}

util.inherits(LevelDbLog, events.EventEmitter);
module.exports = LevelDbLog;


///--- Globals

var SYNC_OPTIONS = { 'sync': true };
var LAST_INDEX_KEY = lastIndexKey();



///--- Helpers

function errToObj(err) {
    return ({
        'name': err.name === undefined ? '(unknown)' : err.name,
        'message': err.message === undefined ? '(none)' : err.message
    });
}

/**
 * Does the actual initing of the leveldb log.  It will emit either ready or
 * error exactly once.
 *
 * TODO: This may be factored out into another place so that the state machine
 * can use it as a storage place too.
 */
function initDb(opts) {
    var self = this;
    var log = self.log;
    //Try to open without creation, if that fails, then we open and write the
    // initial log entry.
    var levelOpts = {
        'createIfMissing': false,
        'keyEncoding': 'binary',
        'valueEncoding': 'json'
    };
    levelup(opts.location, levelOpts, function (err, db) {
        //Unfortunately, the error code for any type of error is 'OpenError',
        // whether it is already open by the process or just doesn't exist.  So
        // we introspect the message (yuck!) to see if the OpenError is the
        // thing we want to ignore.
        if (err && (err.name !== 'OpenError' ||
                    err.message.indexOf('(create_if_missing is false)') ===
                    -1)) {
            log.fatal({ 'error': errToObj(err) }, 'Error opening leveldb');
            return (self.emit('error', err));
        }

        if (!err) {
            log.debug({ 'location': opts.location }, 'Opened leveldb log');
            db.get(LAST_INDEX_KEY, function (err2, value) {
                if (err2) {
                    log.fatal({ 'error': errToObj(err2) },
                              'Error finding index in db');
                    return (self.emit('error', err2));
                }
                self.nextIndex = value + 1;
                self.db = db;
                self.open = true;
                return (self.emit('ready'));
            });
            return;
        }

        //So there was an err because it doesn't exist.  So we create a new
        // one and write the first log entry.
        log.debug({ 'location': opts.location }, 'Creating new leveldb log');
        levelOpts.createIfMissing = true;
        levelup(opts.location, levelOpts, function (err2, dbc) {
            if (err2) {
                log.fatal({ 'error': errToObj(err2) },
                          'Error creating leveldb');
                return (self.emit('error', err2));
            }

            //The Raft paper says that the index should start at one.  Rather
            // than doing that, a fixed [0] ensures that the consistency check
            // will always succeed.
            var firstLogEntry = { 'term': 0, 'index': 0, 'command': 'noop' };
            dbc.batch([
                { 'type': 'put',
                  'key': keyFromInt(0),
                  'value': firstLogEntry
                },
                { 'type': 'put',
                  'key': LAST_INDEX_KEY,
                  'value': 0
                }
            ], SYNC_OPTIONS, function (err3) {
                if (err3) {
                    log.fatal({ 'error': errToObj(err3) },
                              'Error writing first log entry');
                    return (self.emit('error', err3));
                }
                self.nextIndex = 1;
                self.db = dbc;
                self.open = true;
                self.emit('ready');
            });
        });
    });
}


/**
 * LevelDb recommends that the same key type be used for everything in a db.
 * I'm willing to take that advice.  So in the db, all keys are binary buffers
 * and all values are json objects.  We do need to namespace, so we're reserving
 * the first two bytes for a 'type' field.  So here are the current "registered"
 * types:
 *
 * 0x0000 - Log metadata
 * 0x0001 - Log entries
 *
 * If the leveldb is going to be used as a storage place for other things,
 * we'll have to factor this all out into some common place (along with the
 * logic for creating the initial log data).
 */
//TODO: See the jutification, but this is *woefully* insufficient and must be
// changed.  Ideally, I'd like to stick with a regular javascript number (which
// can go up to 2^53 and should be sufficient).
function keyFromInt(i) {
    var b = new Buffer('000100000000', 'hex');
    b.writeUInt32BE(i, 2);
    return (b);
}


//See comment above.  I'm only keeping this function here so that it is close
// to ^^.
function lastIndexKey() {
    //6 bytes of 0
    return (new Buffer('000000000000', 'hex'));
}



///--- API

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

    if (self.open === false) {
        return (process.nextTick(cb.bind(
            null, new error.InternalError('Attempt to use leveldb_log before ' +
                                          'fully initialized'))));
    }

    function end(err, entry) {
        if (ended) {
            if (err) {
                log.error(err, 'err after already ended');
            }
            return;
        }

        if (level !== null) {
            level.removeAllListeners();
            //Note that this destroy is on the iterator *not the db*.
            level.destroy();
        }
        entries.removeAllListeners();
        ended = true;
        return (cb(err, entry));
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

        var ps = new PairsStream({ 'left': entries, 'right': level });

        var outstandingPuts = 0;
        var successfulPuts = 0;
        var psEnded = false;
        var trackingIndex = firstEntry.index + 1;
        var trackingTerm = firstEntry.term;
        var truncated = false;
        var stateMachine = self.stateMachine;

        function tryEnd() {
            if (!psEnded || (outstandingPuts !== successfulPuts)) {
                return;
            }
            return (end());
        }

        function forcePsEnd(err) {
            ps.removeAllListeners();
            return (end(err));
        }

        ps.on('readable', function () {
            var pair;
            while (null !== (pair = ps.read())) {
                var e = pair.left;
                //Walk down to the value for the level db record
                var l = (pair.right === undefined || pair.right === null) ?
                    pair.right : pair.right.value;

                //The entries ending means we can just stop.
                if (e === undefined || e === null) {
                    return (forcePsEnd());
                }

                //Verify that the indexes are strictly increasing
                if (e.index !== trackingIndex) {
                    return (forcePsEnd(new error.InvalidIndexError(sprintf(
                        'entry index isn\'t strictly increasing at %d',
                        e.index))));
                }
                ++trackingIndex;

                //And the term is increasing
                if (e.term < trackingTerm) {
                    return (forcePsEnd(new error.InvalidTermError(sprintf(
                        'term %d isn\'t strictly increasing at index %d',
                        e.term, e.index))));
                }
                trackingTerm = e.term;

                //Verify that the entry index isn't behind raft's commit index
                // (entries that have already been applied to the state machine)
                if (commitIndex > e.index) {
                    return (forcePsEnd(new error.InvalidIndexError(sprintf(
                        'commit index %d is ahead of the entry index %d',
                        commitIndex, e.index))));
                }

                //Verify that the entries don't have a term past raft's term
                if (term < e.term) {
                    return (forcePsEnd(new error.InvalidTermError(sprintf(
                        'request term %d is behind the entry term %d',
                        term, e.term))));
                }

                if (l !== null && l !== undefined && !truncated) {
                    //Sanity checking...
                    if (e.index !== l.index) {
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
                        truncated = true;
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
                    db.batch([
                        { 'type': 'put',
                          'key': keyFromInt(e.index),
                          'value': e
                        },
                        { 'type': 'put',
                          'key': LAST_INDEX_KEY,
                          'value': e.index
                        }
                    ], SYNC_OPTIONS, function (err) {
                        if (err) {
                            return (forcePsEnd(err));
                        }
                        ++successfulPuts;
                        tryEnd();
                    });
                    self.nextIndex = e.index + 1;
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
        entries.removeListener('error', onFirstEntryError);
        entries.removeListener('end', onFirstEntryEnd);

        //A single put.
        if (firstEntry.index === undefined) {
            //TODO: Check that the entry has all the right fields
            firstEntry.index = self.nextIndex;
            ++self.nextIndex;
            //TODO: If this fails, we're going to get holes in the log.
            db.put(keyFromInt(firstEntry.index),
                   firstEntry, { 'sync': true },
                   function (err) {
                       log.debug('ending on single put');
                       return (end(null, firstEntry));
                   });
            return;
        }

        //Otherwise, we start reading from a leveldb iterator.
        level = db.createReadStream({
            'start': keyFromInt(firstEntry.index),
            'end': keyFromInt(self.nextIndex - 1)
        });

        level.once('readable', onFirstLevelReadable);
        level.once('error', onFirstLevelError);
        level.once('end', onFirstLevelEnd);
    }

    entries.once('readable', onFirstEntryReadable);
    entries.once('error', onFirstEntryError);
    entries.once('end', onFirstEntryEnd);
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


LevelDbLog.prototype.close = function close(cb) {
    cb = cb || function () {};
    var self = this;
    if (self.open === false) {
        return (process.nextTick(cb));
    }
    if (self.db.isOpen()) {
        self.db.close(function () {
            self.open = false;
            return (cb());
        });
    }
};
