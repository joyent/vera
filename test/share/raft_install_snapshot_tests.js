// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var helper = require('../helper');
var test = require('nodeunit-plus').test;
var util = require('util');
var vasync = require('vasync');



///--- Globals

var createClusterConfig = helper.createClusterConfig;
var e = helper.e();



///--- Tests

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
                t.deepEqual([ 'raft-0' ], newRaft.cluster.allPeerIds);
                t.equal(4, newRaft.stateMachine.commitIndex);
                t.equal('bang', newRaft.stateMachine.data);
                t.ok(Object.keys(newRaft.outstandingMessages).length === 0);
                helper.readClog(newRaft.clog, function (err, entries) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.equals(5, entries.length);
                    //This is a bit wonky... but it is what it should be.
                    var firstEntryCommand = helper.e(createClusterConfig(
                        'raft-0'))(0, 0).command;
                    t.deepEqual([ firstEntryCommand,
                                  'foo', 'bar', 'baz', 'bang' ],
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
