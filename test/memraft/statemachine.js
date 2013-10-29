// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../../lib/error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

/**
 * A dead-simple state machine that keeps one piece of state.  Any execution
 * that is done will update the state.  If any command is applied out of order,
 * it will throw an internal error.
 */

///--- Functions

function StateMachine(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    var self = this;
    self.log = opts.log;
    self.commitIndex = 0;
    self.data = undefined;

    process.nextTick(function () {
        self.ready = true;
        self.emit('ready');
    });
}

util.inherits(StateMachine, events.EventEmitter);
module.exports = StateMachine;



///--- API

StateMachine.prototype.execute = function (entries, cb) {
    assert.arrayOfObject(entries, 'entries');
    assert.func(cb, 'cb');

    var self = this;
    if (!self.ready) {
        return (process.nextTick(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }

    if (entries.length === 0) {
        return (process.nextTick(cb));
    }

    for (var i = 0; i < entries.length; ++i) {
        var entry = entries[i];
        if (entry.index !== (self.commitIndex + 1)) {
            return (process.nextTick(cb.bind(
                null, new error.InternalError(sprintf(
                    'out of order commit, commit at %d, tried to apply entry ' +
                        'with index %d', self.commitIndex, entry.index)))));
        }
        if (entry.command !== 'noop') {
            self.data = entry.command;
        }
        ++self.commitIndex;
    }

    process.nextTick(cb);
};


///--- For Debugging

StateMachine.prototype.dump = function () {
    var self = this;
    console.log({
        'peers': self.peers,
        'messages': self.messages
    });
};
