/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var deepcopy = require('deepcopy');
var events = require('events');
var error = require('../error');
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

    setImmediate(function () {
        self.ready = true;
        self.emit('ready');
    });
}

util.inherits(StateMachine, events.EventEmitter);
module.exports = StateMachine;



///--- API

StateMachine.prototype.execute = function (entries, cb) {
    assert.object(entries, 'entries');
    assert.func(cb, 'cb');

    var self = this;
    if (!self.ready) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }

    entries.on('readable', function () {
        var entry;
        while (null !== (entry = entries.read())) {
            if (entry.index !== (self.commitIndex + 1)) {
                //A little heavy-handed here...
                entries.removeAllListeners();
                return (cb(new error.InternalError(sprintf(
                    'out of order commit, commit at %d, tried to apply entry ' +
                        'with index %d', self.commitIndex, entry.index))));
            }
            if (entry.command !== 'noop') {
                self.data = entry.command;
            }
            ++self.commitIndex;
        }
    });

    entries.on('end', cb);
};


StateMachine.prototype.snapshot = function () {
    var self = this;
    return ({
        'commitIndex': self.commitIndex,
        'data': deepcopy(self.data)
    });
};


StateMachine.prototype.from = function (snapshot, cb) {
    assert.object(snapshot, 'snapshot');

    var self = this;
    var sm = new StateMachine({ log: self.log });
    sm.commitIndex = snapshot.commitIndex;
    sm.data = snapshot.data;
    setImmediate(cb.bind(null, null, sm));
};



///--- For Debugging

StateMachine.prototype.dump = function () {
    var self = this;
    console.log({
        'commitIndex': self.commitIndex,
        'data': self.data
    });
};
