// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var error = require('./error');
var events = require('events');
var TaskPipe = require('./task_pipe');
var vasync = require('vasync');
var util = require('util');



///--- Globals (some things that should be configurable, but aren't yet)

var TICK_MIN = 5;
var TICK_MAX = 10;
var STATES = {
    'leader': 'leader',
    'candidate': 'candidate',
    'follower': 'follower'
};
var CURRENT_TERM = 'currentTerm';
var VOTED_FOR = 'votedFor';



///--- Functions

function Raft(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.id, 'opts.id');
    assert.arrayOfString(opts.peers, 'opts.peers');
    assert.object(opts.clog, 'opts.clog');
    assert.object(opts.stateMachine, 'opts.stateMachine');
    assert.object(opts.messageBus, 'opts.messageBus');
    assert.object(opts.properties, 'opts.properties');

    var self = this;
    self.log = opts.log.child({ 'id': self.id });
    self.id = opts.id;

    //Filter me out of the peers.
    self.peers = [];
    opts.peers.forEach(function (p) {
        if (p !== opts.id) {
            self.peers.push(p);
        }
    });

    self.clog = opts.clog;
    self.stateMachine = opts.stateMachine;

    self.messageBus = opts.messageBus;
    if (self.messageBus.peers[self.id] === undefined) {
        self.messageBus.register(self);
    }
    self.outstandingMessages = {};

    self.properties = opts.properties;

    //This is set in the resetTicker function.  The ticker is used to transition
    // to a candidate (if follower or candidate), or send heartbeats to
    // followers (if leader).
    self.leaderTimeout = undefined;
    self.leaderId = undefined;
    self.transitioning = false;
    self.majority = Math.floor(self.peers.length / 2) + 1;

    //Pipes for request processing...
    self.requestVotesPipe = getRequestVotesPipe.call(self);
    self.appendEntriesPipe = getAppendEntriesPipe.call(self);

    self.resetTicker();
    self.transitionToFollower();
}

util.inherits(Raft, events.EventEmitter);
module.exports = Raft;



///--- Request Vote

//Serialize all request vote requests.
function getRequestVotesPipe() {
    var self = this;
    var rvp = new TaskPipe({
        'name': '_requestVote',
        'func': _requestVote.bind(self)
    });
    return (rvp);
}


/**
 * This is the actual implementation of request vote.
 */
function _requestVote(req, cb) {
    assert.arrayOfObject(req, 'req');
    assert.ok(req.length === 1);
    req = req[0];

    var self = this;
    var log = self.log;

    var voteGranted = false;
    var currentTerm = self.currentTerm();
    var votedFor = self.votedFor();
    var termChanged = false;

    log.debug({ 'request': req }, 'processing voted for');

    if (req.term < currentTerm) {
        log.debug({ 'currentTerm': currentTerm,
                    'requestTerm': req.term },
                  'Term is out of date in request, dropping it.');
        return (cb(new error.InvalidTermError('outdated term'), voteGranted));
    }

    if (req.term > currentTerm) {
        log.debug({ 'currentTerm': currentTerm,
                    'requestTerm': req.term },
                  'My term is out of date.');
        //Will update this later...
        termChanged = true;
        currentTerm = req.term;
        votedFor = undefined;
        if (self.state === STATES.candidate ||
            self.state === STATES.leader) {
            log.debug({ 'term': currentTerm, 'state': self.state },
                      'Stepping down.');
            self.transitionToFollower();
        }
    }

    //Already voted for this leader, this term.
    if (votedFor === req.candidateId) {
        log.debug({ 'votedFor': votedFor },
                  'already voted for this candidate');
        voteGranted = true;
        return (cb(null, voteGranted));
    }

    var last = self.clog.last();
    voteGranted =
        //Vote hasn't been casted for this term yet AND
        votedFor === undefined &&
        //Your term is greater than my term OR
        ((last.term < req.lastLogTerm) ||
         //We have the same term and your index is as/more up to date
         (last.term === req.lastLogTerm &&
          last.index <= req.lastLogIndex));
    votedFor = voteGranted ? req.candidateId : undefined;

    log.debug({ 'term': currentTerm,
                'voteGranted': voteGranted,
                'termChanged': termChanged,
                'candidateId': req.candidateId,
                'votedFor': votedFor,
                'clogIndex': last.index,
                'reqIndex': req.lastLogIndex }, 'vote response');
    //The only time an update isn't needed is when we didn't cast a vote and
    // the term was updated.
    if (voteGranted || termChanged) {
        self.updateTermAndVotedFor(currentTerm, votedFor, function () {
            if (voteGranted) {
                self.resetTicker();
            }
            return (cb(null, voteGranted));
        });
    } else {
        return (cb(null, voteGranted));
    }
}



///--- Append Entries

function getAppendEntriesPipe() {
    var self = this;
    //TODO: This is a little ugly.  There might be a nicer-looking way to
    // construct a chain.
    var vtas = new TaskPipe({
        'name': '_verifyTermAndStatus',
        'func': _verifyTermAndStatus.bind(self)
    });
    var atc = new TaskPipe({
        'name': '_appendToCLog',
        'func': _appendToCLog.bind(self)
    });
    var ele = new TaskPipe({
        'name': '_executeLatestEntries',
        'func': _executeLatestEntries.bind(self)
    });

    vtas.next = atc;
    atc.next = ele;

    return (vtas);
}


function _verifyTermAndStatus(req, cb) {
    assert.arrayOfObject(req, 'req');
    assert.ok(req.length === 1);
    req = req[0];

    var self = this;
    var log = self.log;
    var currentTerm = self.currentTerm();

    log.debug({ 'request': req }, 'processing append entries');

    function verifyTermAndStatus(_, subcb) {
        if (req.term < currentTerm) {
            log.debug({ 'currentTerm': currentTerm,
                        'requestTerm': req.term },
                      'Term is out of date in request, dropping it.');
            return (subcb(new error.InvalidTermError('outdated term')));
        }
        if (req.term === currentTerm && self.leaderId !== undefined &&
            self.leaderId !== req.leaderId) {
            //TODO: This is obviously not sufficient
            log.fatal({
                'term': req.term,
                'reqLeader': req.leaderId,
                'selfLeader': self.leaderId
            }, 'possible leader conflict!!!!');
            return (subcb(new error.InternalError('leader mismatch')));
        }
        if (self.leaderId === undefined) {
            self.leaderId = req.leaderId;
        }
        if (req.term !== currentTerm) {
            log.debug({ 'currentTerm': currentTerm,
                        'requestTerm': req.term },
                      'My term is out of date.');
            currentTerm = req.term;
            self.updateTermAndVotedFor(req.term, function () {
                return (subcb(null));
            });
        } else {
            return (subcb(null));
        }
    }

    function checkState(_, subcb) {
        if (self.state === STATES.candidate || self.state === STATES.leader) {
            log.debug({ 'term': currentTerm, 'state': self.state },
                      'Stepping down.');
            self.transitionToFollower();
        }
        return (subcb(null));
    }

    function resetTicker(_, subcb) {
        self.resetTicker();
        return (subcb(null));
    }

    vasync.pipeline({
        funcs: [
            verifyTermAndStatus,
            checkState,
            resetTicker
        ],
        arg: {}
    }, function (err) {
        return (cb(err));
    });
}


function _appendToCLog(req, cb) {
    assert.arrayOfObject(req, 'req');
    assert.ok(req.length === 1);
    req = req[0];

    var self = this;
    //This will verify that that the log is consistent before appending.
    self.clog.append(req.entries, cb);
}


function _executeLatestEntries(req, cb) {
    assert.arrayOfObject(req, 'req');
    assert.ok(req.length === 1);
    req = req[0];

    var self = this;
    var index = req.commitIndex;
    self.executeLatestEntries(index, function (err) {
        return (cb(err, req));
    });
}



///--- API

Raft.prototype.currentTerm = function () {
    var self = this;
    return (self.properties.get(CURRENT_TERM));
};


Raft.prototype.votedFor = function () {
    var self = this;
    return (self.properties.get(VOTED_FOR));
};


Raft.prototype.updateTermAndVotedFor = function (term, votedFor, cb) {
    assert.number(term, 'term');
    if ((typeof (votedFor)) === 'function') {
        cb = votedFor;
        votedFor = undefined;
    }
    assert.optionalString(votedFor, 'votedFor');
    assert.func(cb, 'cb');

    var self = this;
    var log = self.log;
    var oldTerm = self.properties.get(CURRENT_TERM);
    var oldVotedFor = self.properties.get(VOTED_FOR);
    //TODO: This is a bad- means we have a race condition somewhere
    if (oldTerm > term) {
        return (process.nextTick(cb));
    }
    //If we don't need to update... don't
    if (oldTerm === term && oldVotedFor === votedFor) {
        return (process.nextTick(cb));
    }
    self.properties.write({
        'currentTerm': term,
        'votedFor': votedFor
    }, function (err) {
        self.leaderId = undefined;
        log.debug({ 'term': term, 'votedFor': votedFor },
                  'updated term/votedFor and cleared leaderId');
        //TODO: Err?
        return (cb(null));
    });
};


Raft.prototype.requestVote = function (req, cb) {
    assert.object(req, 'request');
    assert.number(req.term, 'request.term');
    assert.string(req.candidateId, 'request.candidateId');
    assert.number(req.lastLogIndex, 'request.lastLogIndex');
    assert.number(req.lastLogTerm, 'request.lastLogTerm');

    var self = this;
    var log = self.log;

    log.debug({ 'request': req }, 'Entering requestVote');

    self.requestVotesPipe.append(req, function (err, voteGranted) {
        //We want to return the latest term, no matter what.
        var result = {
            'term': self.currentTerm(),
            'voteGranted': voteGranted
        };
        log.debug({ 'err': err, 'result': result }, 'Leaving requestVote');
        cb(err, result);
    });
};


Raft.prototype.appendEntries = function (req, cb) {
    assert.object(req, 'request');
    assert.number(req.term, 'request.term');
    assert.string(req.leaderId, 'request.leaderId');
    //TODO: Entries should be a stream
    assert.arrayOfObject(req.entries, 'request.entries');
    assert.number(req.commitIndex, 'request.commitIndex');

    var self = this;
    var log = self.log;

    log.debug({ 'request': req }, 'Entering appendEntries');

    self.appendEntriesPipe.append(req, function (err) {
        //We want to return the latest term, no matter what.
        var result = {
            'term': self.currentTerm(),
            'success': err === undefined || err === null
        };
        log.debug({ 'err': err, 'result': result }, 'Leaving appendEntries');
        return (cb(err, result));
    });
};


//TODO: This is (eventually) the thing we need to be as fast as possible.
Raft.prototype.clientRequest = function (req, cb) {
    assert.object(req, 'req');
    assert.func(cb, 'cb');
    //TODO: This obviously isn't the correct datatype.
    assert.string(req.command, 'req.command');

    var self = this;
    var log = self.log;
    var entry = {
        'term': self.currentTerm(),
        'command': req.command
    };

    log.debug({ 'req': req }, 'Entering clientRequest');

    function verifyImLeader(_, subcb) {
        if (self.state !== STATES.leader) {
            return (subcb(new error.NotLeaderError('I\'m not the leader')));
        }
        return (subcb(null));
    }

    function appendToCLog(_, subcb) {
        self.clog.append([ entry ], function (err, res) {
            if (err) {
                return (subcb(err));
            }
            entry.index = res.index;
            return (subcb(null));
        });
    }

    function appendEntry(_, subcb) {
        self.sendAppendEntries(entry.index, subcb);
    }

    function executeEntry(_, subcb) {
        self.executeLatestEntries(entry.index, subcb);
    }

    vasync.pipeline({
        funcs: [
            verifyImLeader,
            appendToCLog,
            appendEntry,
            executeEntry
        ],
        arg: {}
    }, function (err) {
        var result = {
            'leaderId': self.leaderId,
            'entryTerm': entry.term,
            'entryIndex': entry.index,
            'success': err === undefined || err === null
        };
        log.debug({ 'err': err, 'result': result }, 'Leaving clientRequest');
        return (cb(err, result));
    });
};


// -- OK Before
//TODO: Need to verify that we are correctly setting the commit index.  Set at:
//  1) When Follower receives the commitIndex from the leader
//  2) When a Leader has one entry committed from current term
//TODO: Because of the asynchronous fun of node, this is *not* the
// correct way of doing this unless the state machine is smart enough
// to hold onto out of order entries.
//TODO: We should only have one of these going at a time.
Raft.prototype.executeLatestEntries = function (index, cb) {
    assert.number(index, 'index');
    assert.func(cb, 'cb');

    var self = this;
    var log = self.log;

    if (index >= self.clog.nextIndex) {
        var message = 'attempted to execute command past end of clog';
        log.error({
            'index': index,
            'clogNextIndex': self.clog.nextIndex
        }, message);
        return (process.nextTick(cb.bind(null, new Error(message))));
    }

    //Kick out if the index has already been executed.
    var smIndex = self.stateMachine.commitIndex;
    if (index < smIndex) {
        return (process.nextTick(cb));
    }

    //The smIndex + 1 here is because we want to start executing with the next
    // command.  The state machine will reject duplicate commands (it enforces
    // a strict order).
    //The req.commitIndex +1 is because we slice up to, but *not including* the
    // last index.
    self.clog.slice(smIndex + 1, index + 1, function (err, entries) {
        if (err) {
            log.error({ 'err': err,
                        'smIndex': smIndex,
                        'index': index
                      },
                      'error getting log entries');
            return (cb(err));
        }

        if (entries.length < 1) {
            return (cb());
        }

        self.stateMachine.execute(entries, function (err2) {
            if (err2) {
                log.error({ 'err': err2, 'entries': entries },
                          'error executing entries on state machine');
            }
            return (cb(err2));
        });
    });
};


//It's like append entries should gather up all the cbs until it hits
// the index, then cb.  A batch, then is natural.
//TODO: Should we pipeline requests to followers, or should we just throw these
// out as fast as possible?
Raft.prototype.sendAppendEntries = function (index, cb) {
    //TODO: We need to do something about that index (above)
    if ((typeof (index)) === 'function') {
        cb = index;
        index = undefined;
    }

    //TODO: These need to be choked.... right?
    var self = this;
    var log = self.log;
    //For the duration of these calls, we'll take what is currently the end.
    var clogEnd = self.clog.nextIndex;

    //TODO: We should enforce a callback
    if (cb === undefined) {
        log.error('no callback given for appendEntries');
        cb = function () { };
    }

    log.debug('sending appendEntries');

    function getAppendEntries(peer, subcb) {
        var peerIndex = self.peerIndexes[peer];
        //The "-1" is for the consistency check entry.
        self.clog.slice(peerIndex - 1, clogEnd, function (err, entries) {
            //TODO: Check error
            return (subcb(err, {
                'operation': 'appendEntries',
                'term': self.currentTerm(),
                'leaderId': self.leaderId,
                'entries': entries,
                'commitIndex': self.stateMachine.commitIndex
            }));
        });
    }

    var replied = 0;
    var calledBack = false;
    function onResponse(err, from, res) {
        //TODO: What to do about the error?

        function end() {
            if (!calledBack) {
                calledBack = true;
                //TODO: Send Response?
                return (cb());
            }
        }

        if (res.term > self.currentTerm() && self.state !== STATES.follower) {
            return (self.updateTermAndVotedFor(res.term, function () {
                self.transitionToFollower();
                end();
            }));
        }

        if (err && err.name === 'TermMismatchError') {
            --self.peerIndexes[from];
            getAppendEntries(from, function (err2, appendRequest) {
                //TODO: err2?
                self.send(from, appendRequest, onResponse);
            });
            return;
        }

        if (res.success) {
            self.peerIndexes[from] = clogEnd;
        }

        ++replied;
        if (replied >= self.majority) {
            self.resetTicker();
            end();
        }
    }

    self.peers.forEach(function (peer) {
        getAppendEntries(peer, function (err, appendRequest) {
            self.send(peer, appendRequest, onResponse);
        });
    });
};


Raft.prototype.transitionToLeader = function () {
    var self = this;
    var log = self.log;
    log.debug('transitioning to leader');
    self.clearOutstandingMessages();

    var last = self.clog.last();
    var nextIndex = last.index + 1;
    self.peerIndexes = {};
    self.peers.forEach(function (peer) {
        self.peerIndexes[peer] = nextIndex;
    });

    //TODO: See the docs, but only doing this, then accepting client requests
    // will lead to an eventually consistent client read.  It needs to be fixed
    // somehow.
    self.sendAppendEntries();
    self.leaderId = self.id;
    self.state = STATES.leader;
    process.nextTick(function () {
        self.emit('stateChange', self.state);
    });
};


Raft.prototype.transitionToCandidate = function () {
    var self = this;
    var log = self.log;
    log.debug('transitioning to candidate');
    self.clearOutstandingMessages();
    self.state = STATES.candidate;

    //TODO: If I'm already transistioning, just ignore more requests until I've
    // updated term and votedFor.  Note that we should really be choking on
    // updating the file.  I wonder if it's possible to get a transition to
    // candidate (voting for self) and a request vote (voting for someone else)
    // at the same time...
    if (self.transitioning) {
        return;
    }
    self.transitioning = true;

    var newTerm = self.currentTerm() + 1;
    var receivedMajority = false;

    vasync.pipeline({
        arg: {},
        funcs: [
            function updatePersistantState(_, subcb) {
                self.updateTermAndVotedFor(newTerm, self.id, subcb);
            },
            function resetTicket(_, subcb) {
                self.resetTicker();
                subcb();
            },
            function sendRequestVotes(_, subcb) {
                var last = self.clog.last();
                var requestVote = {
                    'operation': 'requestVote',
                    'term': newTerm,
                    'candidateId': self.id,
                    'lastLogIndex': last.index,
                    'lastLogTerm': last.term
                };
                var votesFrom = [ self.id ];

                function onResponse(err, from, res) {
                    //TODO: Check err?
                    if (res.term > self.currentTerm() &&
                        self.state !== STATES.follower) {
                        self.updateTermAndVotedFor(res.term, function () {
                            self.transitionToFollower();
                        });
                        return;
                    }

                    //All the other messages should be cleared after we
                    // transition to leader, but just in case...
                    if (receivedMajority) {
                        return;
                    }

                    if (res.voteGranted && votesFrom.indexOf(from) === -1) {
                        log.debug({ 'from': from }, 'got a vote');
                        votesFrom.push(from);
                    }

                    if (votesFrom.length >= self.majority) {
                        log.debug({
                            'votesFrom': votesFrom,
                            'majority': self.majority
                        }, 'recieved majority vote');
                        self.transitionToLeader();
                        receivedMajority = true;
                    }
                }

                self.peers.forEach(function (peer) {
                    self.send(peer, requestVote, onResponse);
                });

                //Since the transitions out of candidate happen independently,
                // we're officially a candidate after we fire off the messages.
                return (subcb(null));
            }
        ]
    }, function (err) {
        //TODO: Err?
        self.transitioning = false;
        self.emit('stateChange', self.state);
    });
};


Raft.prototype.transitionToFollower = function () {
    var self = this;
    var log = self.log;
    log.debug('transitioning to follower');
    self.clearOutstandingMessages();
    self.state = STATES.follower;
    process.nextTick(function () {
        self.emit('stateChange', self.state);
    });
};



//--- Message Bus

Raft.prototype.send = function (peer, message, cb) {
    assert.string(peer, 'peer');
    assert.object(message, 'message');
    assert.func(cb, 'cb');

    var self = this;
    var log = self.log;

    function onResponse(err, mId, from, res) {
        log.debug({ 'err': err, 'messageId': mId, 'from': from,
                    'response': res }, 'got response from peer');
        if (self.outstandingMessages[mId]) {
            delete (self.outstandingMessages[mId]);
        }
        return (cb(err, from, res));
    }

    var messageId = self.messageBus.send(self.id, peer, message, onResponse);
    self.outstandingMessages[messageId] = 'sent';

    log.debug({ 'peer': peer, 'message': message, 'messageId': messageId },
              'sent message to peer');
};


Raft.prototype.clearOutstandingMessages = function () {
    var self = this;
    var log = self.log;

    log.debug('clearing outstanding messages');

    Object.keys(self.outstandingMessages).forEach(function (messageId) {
        self.messageBus.cancel(messageId);
    });

    self.outstandingMessages = {};
};



///--- Ticker

/**
 * Resets the leader election ticker.  Ticks is optional- if not given it
 * will result in a random number of seconds based on the interval this
 * instance is configured with.
 */
Raft.prototype.resetTicker = function (ticks) {
    assert.optionalNumber(ticks, 'ticks');
    var self = this;
    if (self.state === 'leader') {
        self.leaderTimeout = Math.max(1, TICK_MIN - 1);
    } else {
        self.leaderTimeout = ticks !== undefined ? ticks :
            Math.floor(Math.random() * (TICK_MAX - TICK_MIN + 1)) +
            TICK_MIN;
    }
    self.log.debug({ 'leaderTimeout': self.leaderTimeout },
                   'resetting ticker');
};


/**
 * Tick... tick... tick... BOOM!
 */
Raft.prototype.tick = function () {
    var self = this;
    --self.leaderTimeout;
    self.log.debug({ 'state': self.state,
                     'leaderTimeout': self.leaderTimeout },
                   'ticking');
    if (self.leaderTimeout <= 0) {
        if (self.state === STATES.candidate || self.state === STATES.follower) {
            self.log.debug('leader timed out, transitioning to candidate');
            self.transitionToCandidate();
        } else {
            self.sendAppendEntries();
        }
    }
};
