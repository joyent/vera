// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var helper = require('../helper.js');
var lib = require('../../lib');
var vasync = require('vasync');



///--- Globals

var entryStream = helper.entryStream;
var test = helper.test;
var LOW_LEADER_TIMEOUT = 2;



///--- Helpers

//See lib/raft.js#sendAppendEntries
function ae(req) {
    req.operation = req.operation || 'appendEntries';
    req.term = req.term === undefined ? 0 : req.term;
    req.leaderId = req.leaderId || 'raft-1';
    req.entries = req.entries || entryStream([ 0, 0 ]);
    req.commitIndex = req.commitIndex === undefined ? 0 : req.commitIndex;
    return (req);
}



///--- Tests

test('state after test init and log[0] heartbeat', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function checkInitial(_, subcb) {
                var r = self.raft;
                t.equal(3, r.currentTerm());
                t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                t.equal('raft-1', r.leaderId);
                t.equal('follower', r.state);
                t.equal(4, r.clog.nextIndex);
                t.equal(2, r.stateMachine.commitIndex);
                t.equal('command-2-2', r.stateMachine.data);
                helper.readClog(r.clog, function (err2, entries) {
                    t.equal(4, entries.length);
                    subcb();
                });
            },
            function append(_, subcb) {
                var r = self.raft;
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);
                    return (subcb(err));
                });
            }, function appendAgain(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(5, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(5, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function checkInitial(_, subcb) {
                var r = self.raft;
                helper.readClog(r.clog, function (err2, entries) {
                    t.equal(4, entries.length);
                    t.equal('command-3-3', entries[3].command);
                    subcb();
                });
            },
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 2, 2, 3, 2 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        t.equal('command-3-2', entries[3].command);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 4 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 2, 4, 2 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 5, 3, 6, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-2',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);
                    return (subcb(err));
                });
            }, function appendAgain(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 4, 3, 5, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(6, entries.length);
                        t.deepEqual([ 'noop', 'command-1-1', 'command-2-2',
                                      'command-3-3', 'command-4-3',
                                      'command-5-3' ],
                                    entries.map(function (entry) {
                                        return (entry.command);
                                    }));
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-2',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.success);

                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-2', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(5, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(5, entries.length);
                        t.deepEqual([ 'noop', 'command-1-1', 'command-2-2',
                                      'command-3-3', 'command-4-3' ],
                                    entries.map(function (entry) {
                                        return (entry.command);
                                    }));
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 3,
                    'entries': entryStream([ 3, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(3, r.stateMachine.commitIndex);
                    t.equal('command-3-3', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function becomeLeader(_, subcb) {
                var r = self.raft;
                r.once('stateChange', function (state) {
                    r.transitionToLeader();
                    return (subcb());
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = self.raft;
                t.equal('leader', r.state);
                r.appendEntries(ae({
                    'term': 5,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(5, res.term);
                    t.ok(res.success);

                    t.equal(5, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function becomeLeader(_, subcb) {
                var r = self.raft;
                r.once('stateChange', function (state) {
                    r.transitionToLeader();
                    return (subcb());
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = self.raft;
                t.equal('leader', r.state);
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function becomeCandidate(_, subcb) {
                var r = self.raft;
                r.once('stateChange', function (state) {
                    return (subcb());
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = self.raft;
                t.equal('candidate', r.state);
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function becomeCandidate(_, subcb) {
                var r = self.raft;
                r.once('stateChange', function (state) {
                    subcb();
                });
                r.transitionToCandidate();
            },
            function append(_, subcb) {
                var r = self.raft;
                t.equal('candidate', r.state);
                r.appendEntries(ae({
                    'term': 5,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 0,
                    'entries': entryStream([ 0, 0, 1, 1 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 1, 1, 2, 2 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(5, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(5, entries.length);
                        t.equal('command-4-3', entries[4].command);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 0, 0, 1, 1, 2, 2 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 1, 1, 2, 2, 3, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 3, 5, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(6, entries.length);
                        t.equal('command-4-3', entries[4].command);
                        t.equal('command-5-3', entries[5].command);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 2, 2, 5, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 2,
                    'entries': entryStream([ 3, 3, 4, 2, 5, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': -1,
                    'entries': entryStream([ 3, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': -1,
                    'entries': entryStream([ 3, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 3,
                    'entries': entryStream([ 3, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(4, r.clog.nextIndex);
                    t.equal(3, r.stateMachine.commitIndex);
                    t.equal('command-3-3', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
test('commit index too far in future, no append', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 4,
                    'entries': entryStream([ 3, 3 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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


test('commit index too far in future, one append', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-1',
                    'commitIndex': 5,
                    'entries': entryStream([ 3, 3, 4, 4 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.success === false);

                    t.ok(err);
                    t.equal('InvalidIndexError', err.name);

                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    //We'll still append it even though there were problems.
                    t.equal(5, r.clog.nextIndex);
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(5, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 4,
                    'entries': entryStream([ 3, 3, 4, 3, 5, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(4, r.stateMachine.commitIndex);
                    t.equal('command-4-3', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(6, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 5,
                    'entries': entryStream([ 3, 3, 4, 3, 5, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);

                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(6, r.clog.nextIndex);
                    t.equal(5, r.stateMachine.commitIndex);
                    t.equal('command-5-3', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(6, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;

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
                        'entries': entryStream([ 3, 3, 4, 3 ])
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
                        'entries': entryStream([ 3, 3, 4, 3, 5, 3 ])
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(3, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });
            },
            function checkRaft(_, subcb) {
                var r = self.raft;
                t.equal(3, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-1', r.leaderId);
                t.equal('follower', r.state);
                t.equal(6, r.clog.nextIndex);
                t.equal(5, r.stateMachine.commitIndex);
                t.equal('command-5-3', r.stateMachine.data);
                helper.readClog(r.clog, function (err2, entries) {
                    t.equal(6, entries.length);
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


test('concurrent appends, same future terms', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;

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
                        'entries': entryStream([ 3, 3, 4, 4 ])
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
                        'entries': entryStream([ 3, 3, 4, 4, 5, 4 ])
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(4, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });
            },
            function checkRaft(_, subcb) {
                var r = self.raft;
                t.equal(4, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-2', r.leaderId);
                t.equal('follower', r.state);
                t.equal(6, r.clog.nextIndex);
                t.equal(5, r.stateMachine.commitIndex);
                t.equal('command-5-4', r.stateMachine.data);
                helper.readClog(r.clog, function (err2, entries) {
                    t.equal(6, entries.length);
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


test('concurrent appends, first future term', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;

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
                        'entries': entryStream([ 3, 3, 4, 5 ])
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
                        'entries': entryStream([ 3, 3, 4, 4, 5, 4 ])
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
                var r = self.raft;
                t.equal(5, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-2', r.leaderId);
                t.equal('follower', r.state);
                t.equal(5, r.clog.nextIndex);
                t.equal(4, r.stateMachine.commitIndex);
                t.equal('command-4-5', r.stateMachine.data);
                helper.readClog(r.clog, function (err2, entries) {
                    t.equal(5, entries.length);
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


test('concurrent appends, second future term', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;

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
                        'entries': entryStream([ 3, 3, 4, 4 ])
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
                        'entries': entryStream([ 3, 3, 4, 4, 5, 5 ])
                    }), function (err, res) {
                        t.ok(res);
                        t.equal(5, res.term);
                        t.ok(res.success);
                        tryEnd();
                    });
                });
            },
            function checkRaft(_, subcb) {
                var r = self.raft;
                t.equal(5, r.currentTerm());
                t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                t.equal('raft-2', r.leaderId);
                t.equal('follower', r.state);
                t.equal(6, r.clog.nextIndex);
                t.equal(5, r.stateMachine.commitIndex);
                t.equal('command-5-5', r.stateMachine.data);
                helper.readClog(r.clog, function (err2, entries) {
                    t.equal(6, entries.length);
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


//I don't see any reason not to just let the consistancy check catch this.
test('index goes before first entry (index of -1)', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 0,
                    'entries': entryStream([ -1, -1, 0, 0 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-13',
                    'commitIndex': 0,
                    'entries': entryStream([ 0, 0 ])
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
                    t.equal(2, r.stateMachine.commitIndex);
                    t.equal('command-2-2', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(4, entries.length);
                        subcb();
                    });
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 3,
                    'leaderId': 'raft-1',
                    'commitIndex': 5,
                    'entries': entryStream([ 3, 3, 4, 3, 5, 3 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.success);
                    return (subcb());
                });
            },
            function attemptTruncate(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'term': 4,
                    'leaderId': 'raft-2',
                    'commitIndex': 5,
                    'entries': entryStream([ 3, 3, 4, 4, 5, 4, 6, 4 ])
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
