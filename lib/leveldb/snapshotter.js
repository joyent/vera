// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var deepcopy = require('deepcopy');
var events = require('events');
var error = require('../error');
var lib = require('../leveldb');
var lstream = require('lstream');
var sprintf = require('extsprintf').sprintf;
var Transform = require('stream').Transform;
var util = require('util');
var vasync = require('vasync');



/**
 * Will make the "last" snapshot available.
 */

///--- Functions

function Snapshotter(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.db, 'opts.db');
    assert.optionalObject(opts.raft, 'opts.raft');

    var self = this;
    self.log = opts.log;
    self.db = opts.db;
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

    var db = self.raft.clog.db;
    //Since all the entries that should be in the snapshot are from 0x0001 on,
    // we just start an iterator there and go.
    var rs = db.createReadStream({
        'start': new Buffer('0001', 'hex')
    });
    var otrans = Transform({ objectMode: true });
    otrans._transform = function (data, encoding, subcb) {
        console.log(data);
        this.push(JSON.stringify(data, null, 0) + '\n');
        subcb();
    };
    rs.pipe(otrans);

    setImmediate(cb.bind(null, null, otrans));
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

    var db = self.raft.clog.db;

    // Apply the transform to the db.
    var otrans = Transform({ objectMode: true });
    otrans._transform = function (data, encoding, subcb) {
        if (data === '') {
            return (subcb());
        }
        //This side decodes the json and turns it back into a
        // stream of objects for the db to put.
        var d = JSON.parse(data);
        d.key = new Buffer(d.key);
        this.push(d);
        subcb();
    };
    var ws = db.createWriteStream();

    var ret = {};
    ws.on('close', function () {
        vasync.pipeline({
            'arg': ret,
            'funcs': [
                function readPeersFromDb(_, subcb) {
                    var k = lib.internalPropertyKey('peers');
                    db.get(k, function (err, p) {
                        if (err) {
                            return (subcb(err));
                        }
                        ret.peers = p;
                        subcb();
                    });
                },
                function initStateMachine(_, subcb) {
//TODO: Start here...
                },
                function initClog(_, subcb) {

                }
            ]
        }, function (err) {
            if (err) {
                return (cb(err));
            }
            return (cb(null, ret));
        });
    });

    snapshot.pipe(new lstream()).pipe(ws);
};
