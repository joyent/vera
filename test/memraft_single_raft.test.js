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
    assert.arrayOfString(raft.cluster.peerIds());
    t.ok(raft.cluster.peerIds().indexOf(raft.id) === -1);
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

test('init mem cluster of 1', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initCluster({ 'size': 1, 'electLeader': false }),
            function checkCluster(_, subcb) {
                var c = _.cluster;
                assert.object(c.messageBus, 'c.messageBus');
                assert.object(c.peers, 'c.peers');
                t.equal(1, Object.keys(c.peers).length);
                checkInitalRaft(c.peers['raft-0'], t);
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
            initCluster({ 'size': 1, 'electLeader': false }),
            function toCandidate(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                //Set timeout low for the follower and it should become a
                // candidate on the next tick...
                r0.leaderTimeout = 1;
                r0.tick();
                r0.once('stateChange', function (state) {
                    t.equal('candidate', state);
                    t.equal(undefined, r0.leaderId);
                    assert.ok(r0.leaderTimeout > 0);
                    t.equal(1, r0.currentTerm());
                    t.equal(r0.id, r0.votedFor());
                    t.equal(0, Object.keys(r0.outstandingMessages).length);
                    return (subcb(null));
                });
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
            initCluster({ 'size': 1, 'electLeader': false }),
            function toCandidate(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                //Set timeout low for the follower and it should become a
                // candidate on the next tick...
                r0.leaderTimeout = 1;
                r0.tick();
                r0.on('stateChange', function (state) {
                    r0.removeAllListeners();

                    //Make sure r0 is a candidate and there are 0 messages in
                    //the message bus.  It's tested in more detail above.
                    t.equal('candidate', r0.state);
                    t.equal(0, Object.keys(c.messageBus.messages).length);

                    subcb();
                });
            },
            function toLeader(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];
                r0.on('stateChange', function (state) {
                    r0.removeAllListeners();
                    t.equal('leader', state);
                    subcb();
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

                //Don't need any heartbeats...
                t.equal(0, Object.keys(r0.outstandingMessages).length);
                t.equal('leader', r0.state);
                subcb();
            },
            function maintainLeadership(_, subcb) {
                var c = _.cluster;
                var r0 = c.peers['raft-0'];

                var ticks = r0.leaderTimeout + 1;
                function next() {
                    c.tick(function () {
                        if (--ticks > 0) {
                            return (next());
                        }
                        t.ok(r0.leaderTimeout > 2);
                        subcb();
                    });
                }
                next();
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('random election with one', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initCluster({ 'size': 1 })
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
            initCluster({ 'size': 1 }),
            function clientRequest(_, subcb) {
                var c = _.cluster;
                var l = c.getLeader();

                function onResponse(err, res) {
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

                    subcb();
                }

                l.clientRequest({ 'command': 'foo' }, onResponse);
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
            initCluster({ 'size': 1 }),
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
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err.toString());
        }
        t.done();
    });
});
