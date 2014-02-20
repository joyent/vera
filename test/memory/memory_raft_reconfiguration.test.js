// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('../helper');
var memraft = require('../memory');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var entryStream = helper.entryStream();
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});



///--- Helpers

function checkInitalRaft(raft, t) {
    assert.string(raft.id, 'raft.id');
    assert.object(raft.log, 'raft.log');
    t.equal(undefined, raft.leaderId);
    assert.number(raft.leaderTimeout);
    t.equal(0, raft.currentTerm());
    t.equal(undefined, raft.votedFor());
    assert.arrayOfString(raft.cluster.allPeerIds);
    t.ok(raft.cluster.allPeerIds.indexOf(raft.id) === -1);
    assert.object(raft.clog);
    assert.object(raft.stateMachine);
    assert.object(raft.messageBus);
    t.ok(Object.keys(raft.outstandingMessages).length === 0);
    t.equal('follower', raft.state);
}


function initCluster(opts) {
    opts = opts || {};
    assert.object(opts, 'opts');
    assert.optionalNumber(opts.size, 'opts.size');
    assert.optionalBool(opts.electLeader, 'opts.electLeader');

    opts.log = opts.log || LOG;
    opts.size = opts.size !== undefined ? opts.size : 3;
    opts.electLeader = opts.electLeader !== undefined ? opts.electLeader : true;

    return (function (_, subcb) {
        memraft.cluster(opts, function (err, cluster) {
            if (err) {
                return (subcb(err));
            }
            _.cluster = cluster;
            return (subcb(null, cluster));
        });
    });
}



///--- Tests

test('single raft, not bootstrapped on init', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            function newRaft(_, subcb) {
                memraft.raft({
                    'log': LOG,
                    'id': 'raft-0'
                }, function (err, r) {
                    if (err) {
                        return (subcb(err));
                    }
                    _.raft = r;
                    subcb();
                });
            },
            function tryTick(_, subcb) {
                t.equal('follower', _.raft.state);
                _.raft.tick();
                var newTimeout = _.raft.leaderTimeout;
                _.raft.tick();
                //Ticks should only reset a raft instance that has no
                // configuration.
                t.equal(newTimeout, _.raft.leaderTimeout);
                subcb();
            },
            function tryAppend(_, subcb) {
                _.raft.appendEntries({
                    'term': 0,
                    'commitIndex': 0,
                    'leaderId': 'raft-1',
                    'entries': entryStream([ 0, 0 ])
                }, function (err) {
                    if (!err) {
                        t.fail();
                        return (subcb());
                    }
                    t.equal('NotBootstrappedError', err.name);
                    subcb();
                });
            },
            function tryRequestVote(_, subcb) {
                _.raft.requestVote({
                    'term': 0,
                    'candidateId': 'raft-1',
                    'lastLogIndex': 0,
                    'lastLogTerm': 0
                }, function (err) {
                    if (!err) {
                        t.fail();
                        return (subcb());
                    }
                    t.equal('InvalidPeerError', err.name);
                    subcb();
                });
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err.toString());
        }
        t.done();
    });
});

// Error on single raft instance non-voting as the cluster config (both on init
//   and on reconfiguration)
// Successful, no problems
// Old dies after starting to use old/new, before committing
// Old dies right after new config is propagated to majority
// Old leader isn't in new configuration
// Old leader isn't isn't voting in new configuration
// New leader finishes up the reconfiguration
// Multiple cluster reconfigurations at the same time
// What a raft instance does when it isn't in the new config
// Read only instances shouldn't try to get votes
// Failure tests described in design_rationale.md#What do peers that aren't...?
