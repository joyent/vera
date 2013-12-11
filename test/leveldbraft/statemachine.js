// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../../lib/error');
var levelDbIndex = require('../../lib/leveldb');
var sprintf = require('extsprintf').sprintf;
var util = require('util');



/**
 * A dead-simple, leveldb-backed state machine that keeps one piece of state.
 * Any execution that is done will update the state.  If any command is applied
 * out of order, it will throw an internal error.
 *
 * The reason this is in the test directory is because it would be absurd as the
 * actual state machine.  I'm doing it only so that we have an actual async
 * thing for the leveldb raft tests.
 */

///--- Globals

//In prod, this would be a very bad thing to share the namespace with the
// internal properties.
var propertyKey = levelDbIndex.propertyKey;



///--- Functions

function StateMachine(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.db, 'opts.db');

    var self = this;
    self.log = opts.log;
    self.db = opts.db;

    self.commitIndex = 0;
    self.data = undefined;

    verifyDb.call(self);
}

util.inherits(StateMachine, events.EventEmitter);
module.exports = StateMachine;


///--- Helpers

function verifyDb() {
    var self = this;
    var props = {};

    function ready() {
        self.ready = true;
        self.emit('ready');
    }

    function persistProps() {
        var batch = [];
        Object.keys(props).forEach(function (k) {
            batch.push({ 'type': 'put', 'key': propertyKey(k),
                         'value': props[k] });
        });
        if (batch.length > 0) {
            self.db.batch(batch, function (err) {
                if (err) {
                    return (self.emit('error', err));
                }
                ready();
            });
        } else {
            ready();
        }
    }

    function loadCommitIndex() {
        self.db.get(propertyKey('stateMachineCommitIndex'),
                    function (err, val) {
                        if (err && err.name === 'NotFoundError') {
                            props['stateMachineCommitIndex'] = 0;
                        } else if (err) {
                            return (self.emit('error', err));
                        } else {
                            self.commitIndex = val;
                        }
                        persistProps();
                    });
    }

    function loadData() {
        self.db.get(propertyKey('stateMachineData'),
                    function (err, val) {
                        if (err && err.name !== 'NotFoundError') {
                            return (self.emit('error', err));
                        } else {
                            self.data = val;
                        }
                        loadCommitIndex();
                    });
    }
    loadData();
}



///--- API

StateMachine.prototype.execute = function (entries, cb) {
    assert.object(entries, 'entries');
    assert.func(cb, 'cb');

    var self = this;
    if (!self.ready) {
        return (process.nextTick(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    var nData = self.data;
    var nCommitIndex = self.commitIndex;

    entries.on('readable', function () {
        var entry;
        while (null !== (entry = entries.read())) {
            if (entry.index !== (nCommitIndex + 1)) {
                //A little heavy-handed here...
                entries.removeAllListeners();
                return (cb(new error.InternalError(sprintf(
                    'out of order commit, commit at %d, tried to apply entry ' +
                        'with index %d', self.commitIndex, entry.index))));
            }
            if (entry.command !== 'noop') {
                nData = entry.command;
            }
            ++nCommitIndex;
        }
    });

    entries.on('end', function () {
        var batch = [];
        if (nData !== undefined && nData !== null) {
            batch.push({ 'type': 'put', 'key': propertyKey('stateMachineData'),
                         'value': nData });
        }
        if (nCommitIndex !== undefined && nCommitIndex !== null) {
            batch.push({ 'type': 'put',
                         'key': propertyKey('stateMachineCommitIndex'),
                         'value': nCommitIndex });
        }
        self.db.batch(batch, function (err) {
            if (!err) {
                self.data = nData;
                self.commitIndex = nCommitIndex;
            }
            return (cb(err));
        });
    });
};



///--- For Debugging

StateMachine.prototype.dump = function () {
    var self = this;
    self.db.createReadStream().on('data', console.log);
};
