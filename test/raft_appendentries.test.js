// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('./helper.js');
var memraft = require('./memraft');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});
var LOW_LEADER_TIMEOUT = 2;



///--- Helpers

//TODO: This is duplicated in a couple places.  Move somewhere.
function e(index, term) {
    return ({
        'index': index,
        'term': term,
        'command': index === 0 ? 'noop' : 'command-' + index + '-' + term
    });
}


function initRaft(opts) {
    opts = opts || {};
    assert.object(opts, 'opts');

    opts.log = opts.log || LOG;
    opts.id = opts.id || 'raft-0';
    opts.peers = opts.peers || [ 'raft-1', 'raft-2' ];

    //Need to "naturally" add some log entries, commit to state machines, etc.
    return (function (_, cb) {
        var raft;
        vasync.pipeline({
            funcs: [
                function init(o, subcb) {
                    memraft.raft(opts, function (err, r) {
                        if (err) {
                            return (subcb(err));
                        }
                        raft = r;
                        return (subcb(null));
                    });
                },
                function addEntries(o, subcb) {
                    raft.appendEntries({
                        'operation': 'appendEntries',
                        'term': 3,
                        'leaderId': 'raft-1',
                        'entries': [
                            e(0, 0, 'noop'),
                            e(1, 1, 'one'),
                            e(2, 2, 'two'),
                            e(3, 3, 'three')
                        ],
                        'commitIndex': 2
                    }, subcb);
                },
                function requestVote(o, subcb) {
                    raft.requestVote({
                        'operation': 'requestVote',
                        'candidateId': 'raft-1',
                        'term': 3,
                        'lastLogTerm': 3,
                        'lastLogIndex': 3
                    }, subcb);
                },
                //To get the leader set.
                function assertLeader(o, subcb) {
                    raft.appendEntries({
                        'operation': 'appendEntries',
                        'term': 3,
                        'leaderId': 'raft-1',
                        'entries': [
                            e(3, 3, 'three')
                        ],
                        'commitIndex': 2
                    }, subcb);
                }
            ]
        }, function (err) {
            _.raft = raft;
            //Set the leaderTimout low...
            raft.leaderTimeout = LOW_LEADER_TIMEOUT;
            raft.messageBus.blackholeUnknown = true;
            return (cb(err, raft));
        });
    });
}


//See lib/raft.js#sendAppendEntries
function ae(req) {
    req.operation = req.operation || 'appendEntries';
    req.term = req.term === undefined ? 0 : req.term;
    req.leaderId = req.leaderId || 'raft-1';
    req.entries = req.entries || [ e(0, 0) ];
    req.commitIndex = req.commitIndex === undefined ? 0 : req.commitIndex;
    return (req);
}



///--- Tests

test('initial heartbeat (empty append, empty follower)', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            //The init function does more, so make this on our own
            function initEmptyRaft(_, subcb) {
                memraft.raft({
                    log: LOG,
                    id: 'raft-0',
                    peers: [ 'raft-1', 'raft-2' ]
                }, function (err, r) {
                    if (err) {
                        return (subcb(err));
                    }
                    _.raft = r;
                    r.leaderTimeout = LOW_LEADER_TIMEOUT;
                    return (subcb(null));
                });
            },
            function checkInitial(_, subcb) {
                var r = _.raft;
                t.equal(0, r.currentTerm());
                t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                t.equal(undefined, r.leaderId);
                t.equal('follower', r.state);
                t.equal(1, r.clog.nextIndex);
                t.equal(1, r.clog.clog.length);
                t.equal(0, r.stateMachine.commitIndex);
                t.equal(undefined, r.stateMachine.data);
                subcb();
            },
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({}), function (err, res) {
                    t.ok(res);
                    t.equal(0, res.term);
                    t.ok(res.success);

                    t.equal(0, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(1, r.clog.nextIndex);
                    t.equal(1, r.clog.clog.length);
                    t.equal(0, r.stateMachine.commitIndex);
                    t.equal(undefined, r.stateMachine.data);
                    return (subcb(err));
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


test('state after test init and log[0] heartbeat', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function checkInitial(_, subcb) {
                var r = _.raft;
                t.equal(3, r.currentTerm());
                t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                t.equal('raft-1', r.leaderId);
                t.equal('follower', r.state);
                t.equal(4, r.clog.nextIndex);
                t.equal(4, r.clog.clog.length);
                t.equal(2, r.stateMachine.commitIndex);
                t.equal('command-2-2', r.stateMachine.data);
                subcb();
            },
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb(err));
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


//test('initial heartbeat (empty append, empty follower)', function (t) {
//test('term out of date', function (t) {
//test('new term, new leader', function (t) {
//test('idempotent append', function (t) {
//test('cause truncation', function (t) {
//test('fail with term mismatch', function (t) {
//test('fail with index too far ahead', function (t) {
//test('leader mismatch (a very bad thing)', function (t) {
//test('two successful appends', function (t) {
//test('previously voted in term, update term with append', function (t) {
//test('only commit index update', function (t) {
//test('leader step down', function (t) {
//test('candidate step down, same term', function (t) {
//test('candidate step down, future term', function (t) {
//test('append one, beginning', function (t) {
//test('append one, middle', function (t) {
//test('append one, end', function (t) {
//test('append many, beginning', function (t) {
//test('append many, middle', function (t) {
//test('append many, end', function (t) {
//test('negative commit index in past', function (t) {
//test('commit index in past', function (t) {
//test('commit index in future', function (t) {
//test('commit index too far in future', function (t) {
//test('concurrent appends, same terms', function (t) {
//test('concurrent appends, same future terms', function (t) {
//test('concurrent appends, first future term, function (t) {
//test('concurrent appends, second future term', function (t) {
//test('index goes before first entry (index of -1)', function (t) {
//test('unknown peer', function (t) {
