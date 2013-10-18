// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('./helper.js');
var fs = require('fs');
var memlib = require('./raft');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
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
    t.equal(0, raft.currentTerm);
    t.equal(undefined, raft.votedFor);
    assert.arrayOfString(raft.peers);
    t.ok(raft.peers.indexOf(raft.id) === -1);
    assert.object(raft.clog);
    assert.object(raft.stateMachine);
    assert.object(raft.messageBus);
    t.ok(Object.keys(raft.outstandingMessages).length === 0);
    t.equal('follower', raft.state);
}



///--- Tests

test('init mem cluster of 3', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            function (_, subcb) {
                var opts = {
                    'log': LOG,
                    'size': 3
                };
                memlib.cluster(opts, function (err, cluster) {
                    _.cluster = cluster;
                    subcb();
                });
            },
            function (_, subcb) {
                var c = _.cluster;
                assert.object(c.messageBus, 'c.messageBus');
                assert.object(c.peers, 'c.peers');
                Object.keys(c.peers).forEach(function (p) {
                    checkInitalRaft(c.peers[p], t);
                });
                subcb();
            }
        ]
    }, function (err) {
        t.done();
    });
});


test('transition to candidate', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            function (_, subcb) {
                var opts = {
                    'log': LOG,
                    'size': 3
                };
                memlib.cluster(opts, function (err, cluster) {
                    _.cluster = cluster;
                    subcb();
                });
            },
            function (_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                //Set timeout low for the follower and it should become a
                // candidate on the next tick...
                r0.leaderTimeout = 1;
                r0.tick();

                //Now it should be a candidate...
                t.equal(undefined, r0.leaderId);
                assert.ok(r0.leaderTimeout > 0);
                t.equal(1, r0.currentTerm);
                t.equal(r0.id, r0.votedFor);
                t.equal(2, Object.keys(r0.outstandingMessages).length);
                t.equal('candidate', r0.state);

                //Check what's in the message bus...
                t.equal(2, Object.keys(c.messageBus.messages).length);
                //We assume that the messages are enqueued in order...
                for (var i = 1; i <= 2; ++i) {
                    var m = c.messageBus.messages[i - 1];
                    t.equal(m.from, r0.id);
                    t.equal(m.to, 'raft-' + i);
                    t.equal('requestVote', m.message.operation);
                    t.equal(r0.id, m.message.candidateId);
                    t.equal(1, m.message.term);
                    t.equal(0, m.message.lastLogIndex);
                    t.equal(0, m.message.lastLogTerm);
                }

                subcb();
            }
        ]
    }, function (err) {
        t.done();
    });
});


test('elect initial leader', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            function (_, subcb) {
                var opts = {
                    'log': LOG,
                    'size': 3
                };
                memlib.cluster(opts, function (err, cluster) {
                    _.cluster = cluster;
                    subcb();
                });
            },
            function (_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                //Set timeout low for the follower and it should become a
                // candidate on the next tick...
                r0.leaderTimeout = 1;
                r0.tick();

                //Make sure r0 is a candidate and there are 2 messages in the
                //message bus.  It's tested in more detail above.
                t.equal('candidate', r0.state);
                t.equal(2, Object.keys(c.messageBus.messages).length);

                //Setting the leader timeout on the other two so that we can
                // verify the timeout gets reset.
                c.peers['raft-1'].leaderTimeout = 2;
                c.peers['raft-2'].leaderTimeout = 2;

                subcb();
            },
            function (_, subcb) {
                var c = _.cluster;
                c.messageBus.tick();
                subcb();
            },
            //Hrm... I had to break this out from the previous so that the the
            // full leader election would take place.  This is because of the
            // process.nextTicks in the mem classes.  Perhaps those weren't the
            // best idea...
            function (_, subcb) {
                subcb();
            },
            //...
            //Then I had to add an extra one to get the heartbeat messages
            // in the messagebus.  Almost definitely not the best idea...
            function (_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];

                //Now it should be the leader...
                t.equal(r0.id, r0.leaderId);
                assert.ok(r0.leaderTimeout > 2);
                t.equal(1, r0.currentTerm);
                t.equal(r0.id, r0.votedFor);
                //The heartbeats to assume leadership...
                t.equal(2, Object.keys(r0.outstandingMessages).length);
                t.equal('leader', r0.state);
                r0.peers.forEach(function (p) {
                    t.equal(1, r0.peerIndexes[p]);
                });

                r0.peers.forEach(function (p) {
                    var peer = c.peers[p];
                    t.equal(undefined, peer.leaderId); //Not until heartbeats.
                    t.equal('raft-0', peer.votedFor);
                    t.ok(peer.leaderTimeout > 2);
                    t.equal(1, peer.currentTerm);
                    t.equal('follower', peer.state);
                });

                subcb();
            }
            //TODO: Deliver the heartbeats...
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
