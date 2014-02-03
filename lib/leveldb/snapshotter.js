// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var deepcopy = require('deepcopy');
var events = require('events');
var error = require('../error');
var lib = require('../leveldb');
var lstream = require('lstream');
var sprintf = require('extsprintf').sprintf;
var stream = require('stream');
var util = require('util');
var vasync = require('vasync');


///--- Globals
var PEERS = 'peers';



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



///--- Inner classes

function JsonTransform(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    var self = this;
    self.log = opts.log;
    stream.Transform.call(self, { 'objectMode': true });
}

util.inherits(JsonTransform, stream.Transform);

JsonTransform.prototype._transform = function (data, encoding, cb) {
    var self = this;
    self.push(JSON.stringify(data, null, 0) + '\n');
    cb();
};

function ObjectTransform(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    var self = this;
    self.log = opts.log;
    stream.Transform.call(self, { 'objectMode': true });
}

util.inherits(ObjectTransform, stream.Transform);

ObjectTransform.prototype._transform = function (data, encoding, cb) {
    var self = this;
    if (data === '') {
        return (cb());
    }
    var obj;
    try {
        obj = JSON.parse(data);
    } catch (e) {
        self.log.error({
            'data': data,
            'error': e
        }, 'error parsing json');
        cb(e);
    }
    if (obj.key) {
        obj.key = new Buffer(obj.key);
    }
    self.push(obj);
    cb();
};



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

    //Write the peers to the db
    var peers = self.raft.peers;
    var db = self.raft.clog.db;

    //Passing the peer list in an internal property key.
    db.batch([
        { 'type': 'put', 'key': lib.internalPropertyKey(PEERS), 'value': peers }
    ], function (err) {
        if (err) {
            return (cb(err));
        }

        //Since all the entries that should be in the snapshot are from 0x0001
        // on, we just start an iterator there and go.
        var rs = db.createReadStream({
            'start': new Buffer('0001', 'hex')
        });
        //TODO: Fix this once I track down why stack traces aren't printing.
        var jtrans = new JsonTransform({ log: self.log });
        rs.pipe(jtrans);

        cb(null, jtrans);
    });
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
    var ws = db.createWriteStream();

    var ret = {};
    ws.on('error', function (err) {
        //TODO: What to do with the streams?
        return (cb(err));
    });

    ws.on('close', function () {
        vasync.pipeline({
            'arg': ret,
            'funcs': [
                function readPeersFromDb(_, subcb) {
                    var k = lib.internalPropertyKey(PEERS);
                    db.get(k, function (err, p) {
                        if (err) {
                            return (subcb(err));
                        }
                        ret.peers = p;
                        subcb();
                    });
                },
                function initStateMachine(_, subcb) {
                    self.raft.stateMachine.from(db, function (err, sm) {
                        if (err) {
                            return (subcb(err));
                        }
                        ret.stateMachine = sm;
                        subcb();
                    });
                },
                function initClog(_, subcb) {
                    self.raft.clog.from({
                        'db': db,
                        'stateMachine': ret.stateMachine
                    }, function (err, clog) {
                        if (err) {
                            return (subcb(err));
                        }
                        ret.clog = clog;
                        subcb();
                    });
                }
            ]
        }, function (err) {
            if (err) {
                return (cb(err));
            }
            return (cb(null, ret));
        });
    });

    //TODO: Catch possible errors thrown from transforms here?
    snapshot
        .pipe(new lstream())
        .pipe(new ObjectTransform({ log: self.log }))
        .pipe(ws);
};
