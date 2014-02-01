// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var LOW_LEADER_TIMEOUT = 2;



///--- Helpers

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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
                    t.equal('raft-1', r.leaderId);
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
                    t.equal('raft-1', r.leaderId);
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
                    t.equal('raft-1', r.leaderId);
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
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
                var r = self.raft;
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
            function stepDown(_, subcb) {
                var r = self.raft;
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
            function stepDown(_, subcb) {
                var r = self.raft;
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
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;
                r.requestVote(rv({
                    'candidateId': 'raft-13',
                    'term': 3,
                    'lastLogIndex': 3,
                    'lastLogTerm': 3
                }), function (err, res) {
                    t.ok(res);
                    t.equal(3, res.term);
                    t.ok(res.voteGranted === false);

                    t.ok(err);
                    t.equal('InvalidPeerError', err.name);

                    t.equal(undefined, r.votedFor());
                    t.equal(3, r.currentTerm());
                    t.ok(r.leaderTimeout === LOW_LEADER_TIMEOUT);
                    t.equal('follower', r.state);
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


//Note that in node, nothing is actually concurrent.  What I mean by this is to
// try and simulate what it would look like for 2 request votes to be invoked as
// close in time as possible (say if we had an http server in front and two
// requests came in "at the same time").  Since "next tick" is as close as we
// can get in node, this should be a fair test.  Just calling them might also
// be sufficient.
test('concurrent requests, same term', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;

                var responses = 0;
                function tryEnd() {
                    if (responses === 2) {
                        t.equal('raft-1', r.votedFor());
                        t.equal(3, r.currentTerm());
                        t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                        t.equal('follower', r.state);

                        return (subcb());
                    }
                }

                //Fortunately, node is predictable and will call this first
                // since it was enqueued to next tick first.
                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-1',
                        'term': 3,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted);
                        tryEnd();
                    });
                });

                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-2',
                        'term': 3,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted === false);
                        tryEnd();
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


//The difference between this and the last is that this one will cause the
// term to increase.  Is should be the same as the above...
test('concurrent requests, increasing terms', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;

                var responses = 0;
                function tryEnd() {
                    if (responses === 2) {
                        t.equal('raft-1', r.votedFor());
                        t.equal(4, r.currentTerm());
                        t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                        t.equal('follower', r.state);

                        return (subcb());
                    }
                }

                //Fortunately, node is predictable and will call this first
                // since it was enqueued to next tick first.
                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-1',
                        'term': 4,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted);
                        tryEnd();
                    });
                });

                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-2',
                        'term': 4,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted === false);
                        tryEnd();
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


test('concurrent requests, latter increasing term', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;

                var responses = 0;
                function tryEnd() {
                    if (responses === 2) {
                        t.equal('raft-2', r.votedFor());
                        t.equal(4, r.currentTerm());
                        t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                        t.equal('follower', r.state);

                        return (subcb());
                    }
                }

                //Fortunately, node is predictable and will call this first
                // since it was enqueued to next tick first.
                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-1',
                        'term': 3,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted);
                        tryEnd();
                    });
                });

                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-2',
                        'term': 4,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted);
                        tryEnd();
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


test('concurrent requests, former increasing term', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function vote(_, subcb) {
                var r = self.raft;

                var responses = 0;
                function tryEnd() {
                    if (responses === 2) {
                        t.equal('raft-1', r.votedFor());
                        t.equal(4, r.currentTerm());
                        t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                        t.equal('follower', r.state);

                        return (subcb());
                    }
                }

                //Fortunately, node is predictable and will call this first
                // since it was enqueued to next tick first.
                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-1',
                        'term': 4,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted);
                        tryEnd();
                    });
                });

                setImmediate(function () {
                    r.requestVote(rv({
                        'candidateId': 'raft-2',
                        'term': 3,
                        'lastLogIndex': 3,
                        'lastLogTerm': 3
                    }), function (err, res) {
                        ++responses;
                        t.ok(res);
                        t.ok(res.voteGranted === false);
                        tryEnd();
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
