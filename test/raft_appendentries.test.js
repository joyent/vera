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


test('first append, first commit', function (t) {
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
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'entries': [ e(0, 0), e(1, 0) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(0, res.term);
                    t.ok(res.success);

                    t.equal(0, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(2, r.clog.nextIndex);
                    t.equal(2, r.clog.clog.length);
                    t.equal('command-1-0', r.clog.clog[1].command);
                    t.equal(0, r.stateMachine.commitIndex);
                    t.equal(undefined, r.stateMachine.data);
                    return (subcb(err));
                });
            },
            function commit(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'commitIndex': 1,
                    'entries': [ e(0, 0), e(1, 0) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(0, res.term);
                    t.ok(res.success);

                    t.equal(0, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(2, r.clog.nextIndex);
                    t.equal(2, r.clog.clog.length);
                    t.equal('command-1-0', r.clog.clog[1].command);
                    t.equal(1, r.stateMachine.commitIndex);
                    t.equal('command-1-0', r.stateMachine.data);
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
                    'leaderId': 'raft-1'
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


test('term out of date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 2,
                    'leaderId': 'raft-1'
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal(err.name, 'InvalidTermError');

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('new term, new leader', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-2'
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.success);

                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-2', r.leaderId);
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


test('idempotent append at end', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);
                    return (subcb(err));
                });
            }, function appendAgain(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(5, r.clog.nextIndex);
                    t.equal(5, r.clog.clog.length);
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


test('cause truncation', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                t.equal('command-3-3', r.clog.clog[3].command);
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(2, 2), e(3, 2) ]
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
                    t.equal('command-3-2', r.clog.clog[3].command);
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


test('log term past request term', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 4) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InvalidTermError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('consistency fail with term mismatch', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 2), e(4, 2) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('TermMismatchError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('consistency fail with index too far ahead', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(5, 3), e(6, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('TermMismatchError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('leader mismatch (a very bad thing)', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-2',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InternalError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('two successful appends', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);
                    return (subcb(err));
                });
            }, function appendAgain(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(4, 3), e(5, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(6, r.clog.clog.length);
                    t.deepEqual([ 'noop', 'command-1-1', 'command-2-2',
                                  'command-3-3', 'command-4-3', 'command-5-3' ],
                                r.clog.clog.map(function (entry) {
                                    return (entry.command);
                                }));
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


test('previously voted in term, update term with append', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-2',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.success);

                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-2', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(5, r.clog.nextIndex);
                    t.equal(5, r.clog.clog.length);
                    t.deepEqual([ 'noop', 'command-1-1', 'command-2-2',
                                  'command-3-3', 'command-4-3' ],
                                r.clog.clog.map(function (entry) {
                                    return (entry.command);
                                }));
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


test('only commit index update (like a heartbeat)', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 3,
                    'entries': [ e(3, 3) ]
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
                    t.equal(3, r.stateMachine.commitIndex);
                    t.equal('command-3-3', r.stateMachine.data);
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


test('leader step down', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function becomeLeader(_, subcb) {
                var r = _.raft;
                r.once('stateChange', function (state) {
                    r.transitionToLeader();
                    return (subcb());
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = _.raft;
                t.equal('leader', r.state);
                r.appendEntries(ae({
                    'term': 5,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(5, res.term);
                    t.ok(res.success);

                    t.equal(5, r.currentTerm());
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


test('some other leader tries append', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function becomeLeader(_, subcb) {
                var r = _.raft;
                r.once('stateChange', function (state) {
                    r.transitionToLeader();
                    return (subcb());
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = _.raft;
                t.equal('leader', r.state);
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InternalError', err.name);

                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-0', r.leaderId);
                    t.equal('leader', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('candidate step down, same term', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function becomeCandidate(_, subcb) {
                var r = _.raft;
                r.once('stateChange', function (state) {
                    return (subcb());
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = _.raft;
                t.equal('candidate', r.state);
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.success);

                    t.equal(4, r.currentTerm());
                    t.equal('raft-0', r.votedFor());
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


test('candidate step down, future term', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function becomeCandidate(_, subcb) {
                var r = _.raft;
                r.once('stateChange', function (state) {
                    subcb();
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = _.raft;
                t.equal('candidate', r.state);
                r.appendEntries(ae({
                    'term': 5,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(5, res.term);
                    t.ok(res.success);

                    t.equal(5, r.currentTerm());
                    t.equal(undefined, r.votedFor());
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


test('append one, beginning', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 0,
                    'entries': [ e(0, 0), e(1, 1) ]
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


test('append one, middle', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(1, 1), e(2, 2) ]
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


test('append one, end', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(5, r.clog.nextIndex);
                    t.equal(5, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    t.equal('command-4-3', r.clog.clog[4].command);
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


//This is getting kinda pedantic...
test('append many, beginning', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(0, 0), e(1, 1), e(2, 2) ]
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


test('append many, middle', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(1, 1), e(2, 2), e(3, 3) ]
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


test('append many, end', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 3), e(5, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(6, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    t.equal('command-4-3', r.clog.clog[4].command);
                    t.equal('command-5-3', r.clog.clog[5].command);
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


test('entries out of order', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(2, 2), e(5, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InvalidIndexError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('terms not strictly increasing', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': [ e(3, 3), e(4, 2), e(5, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InvalidTermError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


//I don't see a problem with just not doing anything with a commit index that's
// less than the current commit index.  So, success should be fine here.
test('negative commit index', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': -1,
                    'entries': [ e(3, 3) ]
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


test('commit index in past', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': -1,
                    'entries': [ e(3, 3) ]
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


test('commit index in future', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 3,
                    'entries': [ e(3, 3) ]
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
                    t.equal(3, r.stateMachine.commitIndex);
                    t.equal('command-3-3', r.stateMachine.data);
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


//This may be a common occurance if we cap the number of entries sent with
// append entries, and a follower is too far behind the leader. See the
// docs for reasons behind impl.
test('commit index too far in future', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 4,
                    'entries': [ e(3, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InvalidIndexError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('commit index is in list of updates', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 4,
                    'entries': [ e(3, 3), e(4, 3), e(5, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(6, r.clog.clog.length);
                    t.equal(4, r.stateMachine.commitIndex);
                    t.equal('command-4-3', r.stateMachine.data);
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


test('commit index is end of updates', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 5,
                    'entries': [ e(3, 3), e(4, 3), e(5, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(6, r.clog.clog.length);
                    t.equal(5, r.stateMachine.commitIndex);
                    t.equal('command-5-3', r.stateMachine.data);
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


//Just like in the request votes test, *nothing* in node is concurrent, but
// nextTicking 2 functions is as close as it is going to get.
test('concurrent appends, same terms', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;

                var responses = 0;
                function tryEnd() {
                    ++responses;
                    if (responses === 2) {
                        return (subcb());
                    }
                }

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 3,
                        'leaderId': 'raft-1',
                        'commitIndex': 4,
                        'entries': [ e(3, 3), e(4, 3) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(3, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 3,
                        'leaderId': 'raft-1',
                        'commitIndex': 5,
                        'entries': [ e(3, 3), e(4, 3), e(5, 3) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(3, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });
            },
            function checkRaft(_, subcb) {
                var r = _.raft;
                t.equal(3, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-1', r.leaderId);
                t.equal('follower', r.state);
                t.equal(6, r.clog.nextIndex);
                t.equal(6, r.clog.clog.length);
                t.equal(5, r.stateMachine.commitIndex);
                t.equal('command-5-3', r.stateMachine.data);
                return (subcb());
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('concurrent appends, same future terms', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;

                var responses = 0;
                function tryEnd() {
                    ++responses;
                    if (responses === 2) {
                        return (subcb());
                    }
                }

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 4,
                        'leaderId': 'raft-2',
                        'commitIndex': 4,
                        'entries': [ e(3, 3), e(4, 4) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(4, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 4,
                        'leaderId': 'raft-2',
                        'commitIndex': 5,
                        'entries': [ e(3, 3), e(4, 4), e(5, 4) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(4, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });
            },
            function checkRaft(_, subcb) {
                var r = _.raft;
                t.equal(4, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-2', r.leaderId);
                t.equal('follower', r.state);
                t.equal(6, r.clog.nextIndex);
                t.equal(6, r.clog.clog.length);
                t.equal(5, r.stateMachine.commitIndex);
                t.equal('command-5-4', r.stateMachine.data);
                return (subcb());
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('concurrent appends, first future term', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;

                var responses = 0;
                function tryEnd() {
                    ++responses;
                    if (responses === 2) {
                        return (subcb());
                    }
                }

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 5,
                        'leaderId': 'raft-2',
                        'commitIndex': 4,
                        'entries': [ e(3, 3), e(4, 5) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(5, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 4,
                        'leaderId': 'raft-2',
                        'commitIndex': 5,
                        'entries': [ e(3, 3), e(4, 4), e(5, 4) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(5, res.term); //From ^^
                        t.ok(res.success === false);

                        t.ok(err);
                        t.equal('InvalidTermError', err.name);

                        tryEnd();
                    });
                });
            },
            function checkRaft(_, subcb) {
                var r = _.raft;
                t.equal(5, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-2', r.leaderId);
                t.equal('follower', r.state);
                t.equal(5, r.clog.nextIndex);
                t.equal(5, r.clog.clog.length);
                t.equal(4, r.stateMachine.commitIndex);
                t.equal('command-4-5', r.stateMachine.data);
                return (subcb());
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('concurrent appends, second future term', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;

                var responses = 0;
                function tryEnd() {
                    ++responses;
                    if (responses === 2) {
                        return (subcb());
                    }
                }

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 4,
                        'leaderId': 'raft-2',
                        'commitIndex': 4,
                        'entries': [ e(3, 3), e(4, 4) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(5, res.term); //This could be either
                        t.ok(res.success);
                        tryEnd();
                    });
                });

                process.nextTick(function () {
                    r.appendEntries(ae({
                        'term': 5,
                        'leaderId': 'raft-2',
                        'commitIndex': 5,
                        'entries': [ e(3, 3), e(4, 4), e(5, 5) ]
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(5, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });
            },
            function checkRaft(_, subcb) {
                var r = _.raft;
                t.equal(5, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-2', r.leaderId);
                t.equal('follower', r.state);
                t.equal(6, r.clog.nextIndex);
                t.equal(6, r.clog.clog.length);
                t.equal(5, r.stateMachine.commitIndex);
                t.equal('command-5-5', r.stateMachine.data);
                return (subcb());
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


//I don't see any reason not to just let the consistancy check catch this.
test('index goes before first entry (index of -1)', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 0,
                    'entries': [ e(-1, -1), e(0, 0) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equals('TermMismatchError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('leader not known in peers', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-13',
                    'commitIndex': 0,
                    'entries': [ e(0, 0) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equals('InvalidPeerError', err.name);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(4, r.clog.clog.length);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    return (subcb());
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


test('attempt to truncate below commit index', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function append(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 5,
                    'entries': [ e(3, 3), e(4, 3), e(5, 3) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);
                    return (subcb());
                });
            },
            function attemptTruncate(_, subcb) {
                var r = _.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-2',
                    'commitIndex': 5,
                    'entries': [ e(3, 3), e(4, 4), e(5, 4), e(6, 4) ]
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InternalError', err.name);

                    return (subcb());
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
