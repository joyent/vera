// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../../lib/error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

//TODO: This really needs to be an idempotent interface so that an append of
// data that's already there won't thrown an error.

///--- Functions

function MemLog(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    var self = this;
    self.log = opts.log;
    self.clog = [];
    //TODO: This is wrong, we shouldn't be keeping this around...
    self.prevIndex = null;
    self.prevTerm = null;

    process.nextTick(function () {
        self.emit('ready');
    });
}

util.inherits(MemLog, events.EventEmitter);
module.exports = MemLog;



///--- API

//We want to keep the checking of prevIndex/prevTerm as close as possible to
//the datasource.  I don't believe there's a reason to append blindly here.
MemLog.prototype.append = function (o, cb) {
    var self = this;
    var e = null;

    assert.object(o, 'o');
    //Because (typeof (null)) === 'object'
    assert.optionalNumber(o.prevIndex === null ? undefined : o.prevIndex);
    assert.optionalNumber(o.prevTerm === null ? undefined : o.prevTerm);
    assert.arrayOfObject(o.entries);

    //TODO: This isn't correct behavior.  Really, it should be looking up the
    //term and index.
    if (self.prevIndex !== o.prevIndex) {
        e = new error.IndexMismatchError(sprintf(
            'append failed: sent prev index (%d) did\'t match ' +
                'current index (%d)', o.prevIndex,
            self.prevIndex));
        return (cb(e));
    }
    if (self.prevTerm !== o.prevTerm) {
        e = new error.TermMismatchError(sprintf(
            'append failed: sent prev term (%d) did\'t match ' +
                'current term (%d)', o.prevTerm,
            self.prevTerm));
        return (cb(e));
    }

    var li = o.prevIndex;
    var lt = o.prevTerm;
    var nextIndex = (li === null) ? 0 : li + 1;
    var loopTerm = lt;

    //Verify indexes are in order, terms are strictly increasing
    for (var i = 0; i < o.entries.length; ++i) {
        var entry = o.entries[i];
        if (nextIndex !== entry.index) {
            e = new error.InvalidIndexError(sprintf(
                'entry at index %d should have index %d (was ' +
                    '%d)', i, nextIndex, entry.index));
            return (cb(e));
        }
        if (entry.term < loopTerm) {
            e = new error.InvalidTermError(sprintf(
                'entry at index %d should have had a term ' +
                    'greater than %d (was %d)', i, loopTerm,
                entry.term));
            return (cb(e));
        }
        loopTerm = entry.term;
        ++nextIndex;
    }

    //Now add to the log.
    for (i = 0; i < o.entries.length; ++i) {
        self.clog[o.entries[i].index] = o.entries[i];
        self.prevIndex = o.entries[i].index;
        self.prevTerm = o.entries[i].term;
    }

    return (cb(null));
};


//Truncates up to and including index.  For example:
//truncate([0, 1, 2], 1) => [0]
MemLog.prototype.truncate = function (index, cb) {
    var self = this;
    if (index === 0) {
        self.clog = [];
        self.prevIndex = null;
        self.prevTerm = null;
    } else {
        self.clog.length = index; //Truncate
        self.prevIndex = self.clog[index - 1].index;
        self.prevTerm = self.clog[index - 1].term;
    }
    cb();
};


//Same function signature as Javascript's array.slice, because it's just a
//passthrough with the exception that a cb can be sent in place of end.
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


///--- For Debugging

MemLog.prototype.dump = function () {
    var self = this;
    console.log({
        prevTerm: self.prevTerm,
        prevIndex: self.prevIndex,
        clog: self.clog
    });
};
