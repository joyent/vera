// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var memraft = require('../memraft');
var Snapshotter = require('./snapshotter');
var util = require('util');
var vasync = require('vasync');



///--- Globals

var before = helper.before;
var e = helper.e;
var entryStream = helper.entryStream;
var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memlog-test',
    stream: process.stdout
});



///--- Setup/Teardown

before(function (cb) {
    var self = this;
    vasync.pipeline({
        funcs: [
            function initSingleCluster(_, subcb) {
                memraft.cluster({
                    log: LOG,
                    size: 1,
                    electLeader: true
                }, function (err, cluster) {
                    if (err) {
                        return (subcb(err));
                    }
                    self.cluster = cluster;
                    subcb();
                });
            },
            function initSnapshotter(_, subcb) {
                var snapshotter = new Snapshotter({ log: LOG });
                snapshotter.on('ready', function () {
                    self.snapshotter = snapshotter;
                    subcb();
                });
            }
        ]
    }, function (err) {
        cb(err);
    });
});



///--- Tests only for the memraft...

test('get snapshot', function (t) {
    var self = this;
    var r = self.cluster.peers['raft-0'];
    var s = self.snapshotter;
    vasync.pipeline({
        funcs: [
            function (_, subcb) {
                s.getLatest(r, function (err, snapshot) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.deepEqual({
                        'peerData': [ 'raft-0' ],
                        'stateMachineData': {
                            'commitIndex': 0,
                            'data': undefined
                        },
                        'clogData': [
                            e(0, 0)
                        ]
                    }, snapshot);
                    subcb();
                });
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('apply snapshot to new', function (t) {
    var self = this;
    var r0 = self.cluster.peers['raft-0'];
    var s = self.snapshotter;
    var r1;
    var snapshot;
    vasync.pipeline({
        funcs: [
            function (_, subcb) {
                vasync.forEachPipeline({
                    'func': r0.clientRequest.bind(r0),
                    'inputs': [
                        { 'command': 'foo' },
                        { 'command': 'bar' },
                        { 'command': 'baz' },
                        { 'command': 'bang' }
                    ]
                }, subcb);
            },
            function (_, subcb) {
                //Init a separate, new raft instance.
                memraft.raft({
                    'log': LOG,
                    'id': 'raft-1',
                    'peers': []
                }, function (err, raft) {
                    if (err) {
                        return (subcb(err));
                    }
                    r1 = raft;
                    subcb();
                });
            },
            function (_, subcb) {
                s.getLatest(r0, function (err, snap) {
                    if (err) {
                        return (subcb(err));
                    }
                    snapshot = snap;
                    subcb();
                });
            },
            function (_, subcb) {
                //TODO: This is a hack... not sure where we should be setting
                // this for real.
                r1.snapshotter = s;
                r1.installSnapshot({
                    'snapshot': snapshot
                }, subcb);
            },
            function (_, subcb) {
                //Check everything we can with the newraft instance.
                t.equal('raft-1', r1.id);
                t.equal(undefined, r1.leaderId);
                t.equal(0, r1.currentTerm());
                t.equal(undefined, r1.votedFor());
                //This is a little wonky.  In "real life" the entry for
                // r1 would have been a part of r0's peer list.  But here we're
                // just checking that it
                t.deepEqual([ 'raft-0' ], r1.peers);
                t.equal(4, r1.stateMachine.commitIndex);
                t.equal('bang', r1.stateMachine.data);
                t.deepEqual([ 'noop', 'foo', 'bar', 'baz', 'bang' ],
                            r1.clog.clog.map(function (x) {
                                return (x.command);
                            }));
                t.ok(Object.keys(r1.outstandingMessages).length === 0);
                subcb();
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
