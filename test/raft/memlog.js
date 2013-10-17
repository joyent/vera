// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../../lib/error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');



///--- Functions

function MemLog(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    var self = this;
    self.log = opts.log;
    //The Raft paper says that the index should start at one.  Rather than
    // doing that, a fixed [0] ensures that the consistency check will always
    // succeed.
    self.clog = [ { 'term': 0, 'index': 0, 'data': 'noop' } ];

    //TODO: Should this be a part of the api?
    process.nextTick(function () {
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
MemLog.prototype.append = function (entries, cb) {
    assert.arrayOfObject(entries, 'entries');
    if (entries.length === 0) {
        return (cb(null));
    }

    var self = this;
    var entry = entries[0];
    assert.optionalNumber(entry.index, 'entries[0].index');
    assert.number(entry.term, 'entries[0].term');

    //Append new
    if (entries.length === 1 && entry.index === undefined) {
        entry.index = self.clog.length;
        self.clog.push(entry);
        return (cb(null, entry));
    }

    //Consistency Check
    var centry = self.clog[entry.index]; //Funny pun, haha
    if (centry === undefined || centry.term !== entry.term) {
        return (cb(new error.TermMismatchError(sprintf(
            'at entry %d, command log term %d doesn\'t match %d',
            entry.index, centry.term, entry.term))));
    }

    //Sanity checks...

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
            self.clog.length = e.index;
        }
        self.clog[e.index] = e;
    }

    return (cb(null));
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
    cb(null, self.clog.slice(start, end));
};


MemLog.prototype.last = function () {
    var self = this;
    return (self.clog[self.clog.length - 1]);
};



///--- For Debugging

MemLog.prototype.dump = function () {
    var self = this;
    console.log({
        clog: self.clog
    });
};
