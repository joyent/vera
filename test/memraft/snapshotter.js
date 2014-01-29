// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var deepcopy = require('deepcopy');
var events = require('events');
var error = require('../../lib/error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

/**
 * Will make the "last" snapshot available.
 */

///--- Functions

function Snapshotter(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    var self = this;
    self.log = opts.log;
    self.ready = false;

    setImmediate(function () {
        self.ready = true;
        self.emit('ready');
    });
}

util.inherits(Snapshotter, events.EventEmitter);
module.exports = Snapshotter;



///--- API

/**
 * Returns an opaque object that can be sent to another memraft instance.
 */
Snapshotter.prototype.getLatest = function (raft, cb) {
    assert.object(raft, 'raft');
    assert.func(cb, 'cb');

    var self = this;
    if (!self.ready) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }

    var snapshot = {};

    //Make Peers
    snapshot.peerData = deepcopy(raft.peers);

    //Clone state machine
    snapshot.stateMachineData = raft.stateMachine.snapshot();

    //Clone clog
    snapshot.clogData = raft.clog.snapshot();

    setImmediate(cb.bind(null, null, snapshot));
};


/**
 * Returns an object with:
 * 1. peers: cluster configuration
 * 2. clog: a new command log
 * 3. stateMachine: a new state machine
 */
Snapshotter.prototype.read = function (raft, snapshot, cb) {
    assert.object(raft, 'raft');
    assert.object(snapshot, 'snapshot');
    assert.func(cb, 'cb');

    var stateMachine = raft.stateMachine.from(snapshot.stateMachineData);
    var clog = raft.clog.from(snapshot.clogData, stateMachine);
    setImmediate(cb.bind(null, null, {
        'peers': snapshot.peerData,
        'stateMachine': stateMachine,
        'clog': clog
    }));
};
