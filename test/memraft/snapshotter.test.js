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
    var peers = [ 'raft-0', 'raft-1' ];
    vasync.forEachParallel({
        'inputs': peers.map(function (p) {
            return ({
                'log': LOG,
                'id': p,
                'peers': [ p ]
            });
        }),
        'func': memraft.raft
    }, function (err, res) {
        if (err) {
            return (cb(err));
        }
        self.oldRaft = res.successes[0];
        self.newRaft = res.successes[1];
        //Manually set the old raft to leader so that we can make client
        // requests.
        self.oldRaft.on('stateChange', function (state) {
            cb();
        });
        self.oldRaft.transitionToLeader();
    });
});



///--- Tests only for the memraft...

test('get snapshot', function (t) {
    var self = this;
    var oldRaft = self.oldRaft;
    var snapshotter = oldRaft.snapshotter;
    vasync.pipeline({
        funcs: [
            function (_, subcb) {
                snapshotter.getLatest(function (err, snapshot) {
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
    var oldRaft = self.oldRaft;
    var snapshotter = oldRaft.snapshotter;
    var newRaft = self.newRaft;
    var snapshot;
    vasync.pipeline({
        funcs: [
            function (_, subcb) {
                vasync.forEachPipeline({
                    'func': oldRaft.clientRequest.bind(oldRaft),
                    'inputs': [
                        { 'command': 'foo' },
                        { 'command': 'bar' },
                        { 'command': 'baz' },
                        { 'command': 'bang' }
                    ]
                }, subcb);
            },
            function (_, subcb) {
                snapshotter.getLatest(function (err, snap) {
                    if (err) {
                        return (subcb(err));
                    }
                    snapshot = snap;
                    subcb();
                });
            },
            function (_, subcb) {
                newRaft.installSnapshot({
                    'snapshot': snapshot
                }, subcb);
            },
            function (_, subcb) {
                //Check everything we can with the newraft instance.
                t.equal('raft-1', newRaft.id);
                t.equal(undefined, newRaft.leaderId);
                t.equal(0, newRaft.currentTerm());
                t.equal(undefined, newRaft.votedFor());
                //This is a little wonky.  In "real life" the entry for newRaft
                // would have been a part of r0's peer list.  But here we're
                // just checking that it copies over the peer list.
                t.deepEqual([ 'raft-0' ], newRaft.peers);
                t.equal(4, newRaft.stateMachine.commitIndex);
                t.equal('bang', newRaft.stateMachine.data);
                t.ok(Object.keys(newRaft.outstandingMessages).length === 0);
                helper.readClog(newRaft.clog, function (err, entries) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.equals(5, entries.length);
                    t.deepEqual([ 'noop', 'foo', 'bar', 'baz', 'bang' ],
                                entries.map(function (x) {
                                    return (x.command);
                                }));
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

// Apply snapshot, having had voted for some raft during some term
