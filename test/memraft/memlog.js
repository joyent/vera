// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../../lib/error');
var helper = require('../helper');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

/**
 * This is an in-memory version of a raft log.  The api is simple, but diverges
 * slightly from what the Raft paper implies.  Since the last term and last
 * index are separate in the client api, it would have made sense to make the
 * append api take a prevLogIndex and prevLogTerm.  Rather than separating them
 * out, I'm just including the full entry as the first element of the entries.
 * That way if we add things (for example, a cumulative hash) we don't have to
 * change types everywhere.  IMO, it makes for a simpler interface for append.
 *
 * The memlog keeps track of the stateMachine so that it can verify safety
 * during log truncation.  It was either that or pass the current commit index
 * along on each append, which would have made it confusing with the request
 * commit index.
 *
 * The other two apis (slice and last) are used to get a slice of the log entry
 * and to get the last entry in this log, respectively.
 *
 * It's intentional that slice takes a callback whereas last doesn't.  It is
 * expected that anything that wants the last entry wants it "now".
 *
 * All those process.nextTicks make sure that this is "fully asynchronous",
 * which seems silly for an in-memory thing, but it should catch weird aync bugs
 * in the raft class.  That's the hope, anyways.  Also see:
 * http://nodejs.org/api/process.html#process_process_nexttick_callback
 */

///--- Functions

function MemLog(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.stateMachine, 'opts.stateMachine');

    var self = this;
    self.log = opts.log;
    self.stateMachine = opts.stateMachine;
    //The Raft paper says that the index should start at one.  Rather than
    // doing that, a fixed [0] ensures that the consistency check will always
    // succeed.
    self.clog = [ { 'term': 0, 'index': 0, 'command': 'noop' } ];
    self.nextIndex = self.clog.length;
    self.ready = false;

    process.nextTick(function () {
        self.ready = true;
        self.emit('ready');
    });
}

util.inherits(MemLog, events.EventEmitter);
module.exports = MemLog;



///--- API

/**
 * Api works as follows:
 *   - Single entry with term and index is a consistency check (ping)
 *   - Single entry with no index gets appended, the index is returned in
 *     response.
 *   - Many entries, first entry is a consistency check.  Truncation happens
 *     if the consistency check passes, but the list of entries diverge.
 *   - If entries are a sublist of the log, nothing happens.
 */
MemLog.prototype.append = function (opts, cb) {
    assert.object(opts, 'opts');
    assert.number(opts.commitIndex, 'opts.commitIndex');
    assert.number(opts.term, 'opts.term');
    assert.object(opts.entries, 'opts.entries');

    var self = this;
    var log = self.log;
    var commitIndex = opts.commitIndex;
    var term = opts.term;
    var entriesStream = opts.entries;
    var entries = [];

    //Just read everything up-front.  This could be refactored to share code
    // with the leveldb log class, but I wanted to keep this as independant
    // as possible... for now.
    entriesStream.on('readable', function () {
        var entry;
        while (null !== (entry = entriesStream.read())) {
            entries.push(entry);
        }
    });

    entriesStream.on('end', function () {
        var entry = entries[0];

        if (entries.length === 0) {
            return (cb());
        }
        assert.optionalNumber(entry.index, 'entries[0].index');
        assert.number(entry.term, 'entries[0].term');
        if (!self.ready) {
            return (cb(new error.InternalError('I wasn\'t ready yet.')));
        }

        //Append new
        if (entries.length === 1 && entry.index === undefined) {
            entry.index = self.clog.length;
            self.clog.push(entry);
            self.nextIndex = self.clog.length;
            return (cb(null, entry));
        }

        //Consistency Check
        var centry = self.clog[entry.index]; //Funny pun, haha
        if (centry === undefined || centry.term !== entry.term) {
            return (cb(new error.TermMismatchError(sprintf(
                'at entry %d, command log term %s doesn\'t match %d',
                entry.index,
                centry === undefined ? '[undefined]' : '' + entry.term,
                entry.term))));
        }

        //Sanity checks...
        var lastEntry = entries[entries.length - 1];
        if (commitIndex > lastEntry.index) {
            return (cb(new error.InvalidIndexError(sprintf(
                'commit index %d is ahead of the last log entry ' +
                    'index %d', commitIndex, lastEntry.index))));
        }

        //Since we make sure terms are strictly increasing below, we only need
        // to check here that the last term isn't greater than the request term.
        if (term < lastEntry.term) {
            return (cb(new error.InvalidTermError(sprintf(
                'request term %d is behind the last log term %d',
                term, lastEntry.term))));
        }

        //Verify indexes are in order, terms are strictly increasing
        var loopTerm = entry.term;
        for (var i = 1; i < entries.length; ++i) {
            var e = entries[i];
            if (e.index !== (entry.index + i)) {
                return (cb(new error.InvalidIndexError(sprintf(
                    'index isn\'t strictly increasing at %d', i))));
            }
            if (e.term < loopTerm) {
                return (cb(new error.InvalidTermError(sprintf(
                    'term isn\'t strictly increasing at %d', i))));
            }
            loopTerm = e.term;
        }

        //Since truncation isn't safe unless the terms diverge, we have to
        // look at each entry.
        for (i = 1; i < entries.length; ++i) {
            e = entries[i];
            //Truncate if necessary...
            if (self.clog[e.index] && self.clog[e.index].term != e.term) {
                //Up until now, all the records should have been read-and-verify
                // only.  Since we're at the point where we'll actually do
                // damage (truncation), we do some sanity checking.
                if (self.stateMachine.commitIndex >= e.index) {
                    var message = sprintf(
                        'attempt to truncate before state machine\'s ' +
                            'commit index', i);
                    log.error({
                        'stateMachineIndex': self.stateMachine.commitIndex,
                        'oldEntry': self.clog[e.index],
                        'newEntry': e
                    }, message);
                    return (cb(new error.InternalError(message)));
                }
                self.clog.length = e.index;
            }
            self.clog[e.index] = e;
        }

        self.nextIndex = self.clog.length;
        return (cb());
    });
};


//Same function signature as Javascript's array.slice.
MemLog.prototype.slice = function (start, end, cb) {
    assert.number(start, 'index');
    if ((typeof (end)) === 'function') {
        cb = end;
        end = undefined;
    }
    assert.optionalNumber(end, 'index');
    assert.func(cb, 'cb');
    var self = this;
    if (!self.ready) {
        return (process.nextTick(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    return (process.nextTick(
        cb.bind(null, null, helper.memStream(self.clog.slice(start, end)))));
};


MemLog.prototype.last = function () {
    var self = this;
    if (!self.ready) {
        throw new error.InternalError('I wasn\'t ready yet.');
    }
    return (self.clog[self.clog.length - 1]);
};



///--- For Debugging

MemLog.prototype.dump = function () {
    var self = this;
    console.log({
        clog: self.clog
    });
};
