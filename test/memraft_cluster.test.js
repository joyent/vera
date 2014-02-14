// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var memraft = require('./memraft');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

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

test('init mem cluster of 3', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initCluster({ 'electLeader': false }),
            function checkCluster(_, subcb) {
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
            initCluster({ 'electLeader': false }),
            function toCandidate(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                //Set timeout low for the follower and it should become a
                // candidate on the next tick...
                r0.leaderTimeout = 1;
                r0.once('stateChange', function (state) {
                    t.equal('candidate', state);
                    return (subcb(null));
                });
                r0.tick();
            },
            function checkCluster(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                r0.removeAllListeners();

                //Now it should be a candidate...
                t.equal(undefined, r0.leaderId);
                t.ok(r0.leaderTimeout > 0);
                t.equal(1, r0.currentTerm());
                t.equal(r0.id, r0.votedFor());
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
            initCluster({ 'electLeader': false }),
            function toCandidate(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                //Set timeout low for the follower and it should become a
                // candidate on the next tick...
                r0.leaderTimeout = 1;
                r0.tick();
                r0.on('stateChange', function (state) {
                    r0.removeAllListeners();

                    //Make sure r0 is a candidate and there are 2 messages in
                    //the message bus.  It's tested in more detail above.
                    t.equal('candidate', r0.state);
                    t.equal(2, Object.keys(c.messageBus.messages).length);

                    //Setting the leader timeout on the other two so that we can
                    // verify the timeout gets reset.
                    c.peers['raft-1'].leaderTimeout = 2;
                    c.peers['raft-2'].leaderTimeout = 2;

                    subcb();
                });
            },
            function toLeader(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                var ticked = false;
                var stateChanged = false;
                function tryEnd() {
                    if (stateChanged && ticked) {
                        subcb();
                    }
                }
                r0.on('stateChange', function (state) {
                    r0.removeAllListeners();
                    t.equal('leader', state);
                    stateChanged = true;
                    tryEnd();
                });
                c.messageBus.tick(function () {
                    ticked = true;
                    tryEnd();
                });
            },
            function checkCluster(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];

                //Now it should be the leader...
                t.equal(r0.id, r0.leaderId);
                assert.ok(r0.leaderTimeout > 2);
                t.equal(1, r0.currentTerm());
                t.equal(r0.id, r0.votedFor());
                //The heartbeats to assume leadership...
                t.equal(2, Object.keys(r0.outstandingMessages).length);
                t.equal('leader', r0.state);

                r0.cluster.allPeerIds.forEach(function (p) {
                    t.equal(1, r0.peerNextIndexes[p]);

                    var peer = c.peers[p];
                    t.equal(undefined, peer.leaderId); //Not until heartbeats.
                    t.equal('raft-0', peer.votedFor());
                    t.ok(peer.leaderTimeout > 2);
                    t.equal(1, peer.currentTerm());
                    t.equal('follower', peer.state);
                });

                //Setting the leader timeout on the other two so that we can
                // verify the timeout gets reset with the first appendEntries.
                c.peers['raft-1'].leaderTimeout = 2;
                c.peers['raft-2'].leaderTimeout = 2;

                //Cause heartbeats to be delivered...
                c.messageBus.tick(subcb);
            },
            function checkHeartbeatResults(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];

                t.equal(0, Object.keys(c.messageBus.messages).length);

                r0.cluster.allPeerIds.forEach(function (p) {
                    t.equal(1, r0.peerNextIndexes[p]);

                    var peer = c.peers[p];
                    t.equal('raft-0', peer.leaderId);
                    t.equal('raft-0', peer.votedFor());
                    t.ok(peer.leaderTimeout > 2);
                    t.equal(1, peer.currentTerm());
                    t.equal('follower', peer.state);
                });

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


test('random election', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initCluster()
        ]
    }, function (err) {
        if (err) {
            t.fail(err.toString());
        }
        t.done();
    });
});


test('one client request', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initCluster(),
            function clientRequest(_, subcb) {
                var c = _.cluster;
                var l = c.getLeader();

                var responseCalled = false;
                function onResponse(err, res) {
                    responseCalled = true;

                    //Check the Response
                    t.ok(res.success);
                    t.equal(1, res.entryIndex);
                    t.equal(l.currentTerm(), res.entryTerm);
                    t.equal(l.id, res.leaderId);

                    //Check the leader
                    t.equal(2, l.clog.nextIndex);
                    t.equal('foo', l.clog.clog[1].command);
                    t.equal('foo', l.stateMachine.data);
                    t.equal(1, l.stateMachine.commitIndex);

                    //Check the peers (just append to the log)
                    l.cluster.allPeerIds.forEach(function (p) {
                        var peer = c.peers[p];
                        t.equal(2, peer.clog.nextIndex);
                        t.equal('foo', peer.clog.clog[1].command);
                        t.equal(undefined, peer.stateMachine.data);
                        t.equal(0, peer.stateMachine.commitIndex);
                    });

                    subcb();
                }

                l.clientRequest({ 'command': 'foo' }, onResponse);

                //Tick the state machine until we get a response.
                var x = 0;
                function next() {
                    //Safety net...
                    if (x++ === 100) {
                        subcb(new Error('didn\'t complete client request in ' +
                                        '100 ticks'));
                    }
                    if (!responseCalled) {
                        c.tick(next);
                    }
                }
                next();
            },
            function propagateCommitIndex(_, subcb) {
                var c = _.cluster;
                var l = c.getLeader();
                var apeer = c.peers[l.cluster.allPeerIds[0]];

                function onIndexChange() {
                    //Check Peers
                    l.cluster.allPeerIds.forEach(function (p) {
                        var peer = c.peers[p];
                        t.equal(2, peer.clog.nextIndex);
                        t.equal('foo', peer.clog.clog[1].command);
                        t.equal('foo', peer.stateMachine.data);
                        t.equal(1, peer.stateMachine.commitIndex);
                    });
                    return (subcb());
                }

                var x = 0;
                function next() {
                    if (x++ === 100) {
                        return (subcb(new Error('entry never propagated to ' +
                                                'peer state machine')));
                    }
                    if (apeer.stateMachine.commitIndex === 1) {
                        return (onIndexChange());
                    }
                    c.tick(next);
                }
                next();
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err.toString());
        }
        t.done();
    });
});


//Just like in request vote, setImmediate is the closest thing in node
// that I'd call "parallel".
test('parallel client requests', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initCluster(),
            function clientRequest(_, subcb) {
                var c = _.cluster;
                var l = c.getLeader();

                var responses = 0;
                function tryEnd() {
                    ++responses;
                    if (responses === 2) {
                        //Check the leader
                        t.equal(3, l.clog.nextIndex);
                        t.equal('foo', l.clog.clog[1].command);
                        t.equal('bar', l.clog.clog[2].command);
                        t.equal('bar', l.stateMachine.data);
                        t.equal(2, l.stateMachine.commitIndex);

                        //Check the peers (just append to the log)
                        l.cluster.allPeerIds.forEach(function (p) {
                            var peer = c.peers[p];
                            t.equal(3, peer.clog.nextIndex);
                            t.equal('foo', peer.clog.clog[1].command);
                            t.equal('bar', peer.clog.clog[2].command);
                            t.equal(undefined, peer.stateMachine.data);
                            //Commit indexes still haven't been propagated.
                            t.equal(0, peer.stateMachine.commitIndex);
                        });
                        subcb();
                    }
                }

                function onFooResponse(err, res) {
                    //Check the Response
                    t.ok(res.success);
                    t.equal(1, res.entryIndex);
                    t.ok(l.currentTerm() >= res.entryTerm);
                    t.equal(l.id, res.leaderId);

                    tryEnd();
                }

                function onBarResponse(err, res) {
                    //Check the Response
                    t.ok(res.success);
                    t.equal(2, res.entryIndex);
                    t.equal(l.currentTerm(), res.entryTerm);
                    t.equal(l.id, res.leaderId);

                    tryEnd();
                }

                setImmediate(function () {
                    l.clientRequest({ 'command': 'foo' }, onFooResponse);
                });

                setImmediate(function () {
                    l.clientRequest({ 'command': 'bar' }, onBarResponse);
                });

                //Tick the state machine until we get responses.
                var x = 0;
                function next() {
                    //Safety net...
                    if (x++ === 100) {
                        subcb(new Error('didn\'t complete client request in ' +
                                        '100 ticks'));
                    }
                    if (responses < 2) {
                        c.tick(next);
                    }
                }
                next();
            },
            function propagateCommitIndex(_, subcb) {
                var c = _.cluster;
                var l = c.getLeader();
                var apeer = c.peers[l.cluster.allPeerIds[0]];

                function onIndexChange() {
                    //Check Peers
                    l.cluster.allPeerIds.forEach(function (p) {
                        var peer = c.peers[p];
                        t.equal(3, peer.clog.nextIndex);
                        t.equal('bar', peer.stateMachine.data);
                        t.equal(2, peer.stateMachine.commitIndex);
                    });
                    return (subcb());
                }

                var x = 0;
                function next() {
                    if (x++ === 100) {
                        return (subcb(new Error('entry never propagated to ' +
                                                'peer state machine')));
                    }
                    if (apeer.stateMachine.commitIndex === 2) {
                        return (onIndexChange());
                    }
                    c.tick(next);
                }
                next();
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err.toString());
        }
        t.done();
    });
});
