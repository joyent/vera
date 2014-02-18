// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var deepcopy = require('deepcopy');
var events = require('events');
var error = require('../error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

/**
 * Will make the "last" snapshot available.
 */

///--- Functions

function Snapshotter(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.optionalObject(opts.raft, 'opts.raft');

    var self = this;
    self.log = opts.log;
    self.ready = false;
    self.raft = opts.raft;

    setImmediate(function () {
        self.ready = true;
        self.emit('ready');
    });
}

util.inherits(Snapshotter, events.EventEmitter);
module.exports = Snapshotter;



///--- API

/**
 * A snapshotter needs a raft and a raft needs a snapshotter.  We trust raft
 * more, so raft gets to set itself into the snapshotter, rather than the other
 * way around.
 */
Snapshotter.prototype.setRaft = function (raft) {
    assert.object(raft, 'raft');
    this.raft = raft;
};


/**
 * Returns an opaque object that can be sent to another memraft instance.
 */
Snapshotter.prototype.getLatest = function (cb) {
    assert.func(cb, 'cb');

    var self = this;
    if (!self.ready) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    if (!self.raft) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('Snapshotter has no raft.'))));
    }

    var raft = self.raft;
    var snapshot = {};

    //TODO: If we want this to work across a network, we're going to have to
    // be smarter about how we package this up.  It's fine for now.

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
Snapshotter.prototype.read = function (snapshot, cb) {
    assert.object(snapshot, 'snapshot');
    assert.func(cb, 'cb');

    var self = this;
    if (!self.ready) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    if (!self.raft) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('Snapshotter has no raft.'))));
    }

    var raft = self.raft;
    raft.stateMachine.from(snapshot.stateMachineData, function (err, sm) {
        if (err) {
            return (cb(err));
        }
        raft.clog.from(snapshot.clogData, sm, function (err2, clog) {
            if (err) {
                return (cb(err));
            }
            cb(null, {
                'stateMachine': sm,
                'clog': clog
            });
        });
    });
};
