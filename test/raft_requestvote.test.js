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


//See lib/raft.js#transitionToCandidate
function rv(req) {
    req.operation = req.operation || 'requestVote';
    req.term = req.term === undefined ? 0 : req.term;
    req.candidateId = req.candidateId || 'raft-1';
    req.lastLogIndex = req.lastLogIndex === undefined ? 0 : req.lastLogIndex;
    req.lastLogTerm = req.lastLogTerm === undefined ? 0 : req.lastLogTerm;
    return (req);
}


///--- Tests

test('same term, get vote', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.voteGranted);

                    t.equal('raft-1', r.votedFor());
                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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


test('update term, get vote', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 4,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.voteGranted);

                    t.equal('raft-1', r.votedFor());
                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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
            function vote(_, subcb) {
                var r = _.raft;
                var term = r.currentTerm();
                r.requestVote(rv({
                    'term': 2,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3   //Impossible, I know...
                }), function (err, res) {
                    t.ok(res);
                    t.equal(term, res.term);
                    t.ok(res.voteGranted === false);

                    t.ok(err);
                    t.equal(LOW_LEADER_TIMEOUT, r.leaderTimeout);
                    t.equal(term, r.currentTerm());
                    t.ok(undefined === r.votedFor());
                    t.ok(undefined === r.leaderId);
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


test('last log index not as up to date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                var term = r.currentTerm();
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 1,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(term, res.term);
                    t.ok(res.voteGranted === false);

                    t.equal(LOW_LEADER_TIMEOUT, r.leaderTimeout);
                    t.equal(term, r.currentTerm());
                    t.ok(undefined === r.votedFor());
                    t.ok(undefined === r.leaderId);
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


test('last log term not as up to date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                var term = r.currentTerm();
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 2
                }), function (err, res) {
                    t.ok(res);
                    t.equal(term, res.term);
                    t.ok(res.voteGranted === false);

                    t.equal(LOW_LEADER_TIMEOUT, r.leaderTimeout);
                    t.equal(term, r.currentTerm());
                    t.ok(undefined === r.votedFor());
                    t.ok(undefined === r.leaderId);
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


test('same term, log completely out of date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 2,
                    'lastLogTerm': 2
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.voteGranted === false);

                    t.equal(undefined, r.votedFor());
                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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


test('term more up to date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 4,
                    'lastLogIndex': 1,
                    'lastLogTerm': 4
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.voteGranted);

                    t.equal('raft-1', r.votedFor());
                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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


test('index more up to date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 4,
                    'lastLogIndex': 7,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.voteGranted);

                    t.equal('raft-1', r.votedFor());
                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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


test('update term, log out of date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 4,
                    'lastLogIndex': 2,
                    'lastLogTerm': 2
                }), function (err, res) {
                    t.ok(res);
                    t.equal(4, res.term);
                    t.ok(res.voteGranted === false);

                    t.equal(undefined, r.votedFor());
                    t.equal(4, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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


test('already voted for candidate', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.ok(res.voteGranted);
                    return (subcb(err));
                });
            },
            function anotherVote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.voteGranted);

                    t.equal('raft-1', r.votedFor());
                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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


test('already voted for another candidate, log up to date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.ok(res.voteGranted);
                    return (subcb(err));
                });
            },
            function anotherVote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'candidateId': 'raft-2',
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.voteGranted === false);

                    t.equal('raft-1', r.votedFor());
                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);

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


test('already voted for another candidate, log out of date', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.ok(res.voteGranted);
                    return (subcb(err));
                });
            },
            function anotherVote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'candidateId': 'raft-2',
                    'term': 3,
                    'lastLogIndex': 2,
                    'lastLogTerm': 2
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.voteGranted === false);

                    t.equal('raft-1', r.votedFor());
                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);

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


test('step down from leader', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function becomeLeader(_, subcb) {
                var r = _.raft;
                r.once('stateChange', function (state) {
                    r.transitionToLeader();
                    subcb();
                });
                r.transitionToCandidate();
            },
            function stepDown(_, subcb) {
                var r = _.raft;
                t.equal('leader', r.state);
                var newTerm = r.currentTerm() + 1;
                r.requestVote(rv({
                    'term': newTerm,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(newTerm, res.term);
                    t.ok(res.voteGranted);

                    t.equal('raft-1', r.votedFor());
                    t.equal(newTerm, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);

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


test('step down from candidate', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function becomeLeader(_, subcb) {
                var r = _.raft;
                r.once('stateChange', function (state) {
                    subcb();
                });
                r.transitionToCandidate();
            },
            function stepDown(_, subcb) {
                var r = _.raft;
                t.equal('candidate', r.state);
                var newTerm = r.currentTerm() + 1;
                r.requestVote(rv({
                    'term': newTerm,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(newTerm, res.term);
                    t.ok(res.voteGranted);

                    t.equal('raft-1', r.votedFor());
                    t.equal(newTerm, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);

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


test('candidate not in known peers', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            initRaft(),
            function vote(_, subcb) {
                var r = _.raft;
                r.requestVote(rv({
                    'candidateId': 'raft-13',
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.voteGranted === false);

                    t.equal(undefined, r.votedFor());
                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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
