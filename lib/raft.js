// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var Cluster = require('./cluster');
var deepcopy = require('deepcopy');
var error = require('./error');
var events = require('events');
var sprintf = require('extsprintf').sprintf;
var stream = require('stream');
var memstream = require('./memstream');
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
    assert.object(opts.clog, 'opts.clog');
    assert.object(opts.stateMachine, 'opts.stateMachine');
    assert.object(opts.messageBus, 'opts.messageBus');
    assert.object(opts.snapshotter, 'opts.snapshotter');
    assert.object(opts.properties, 'opts.properties');

    var self = this;
    self.log = opts.log.child({ 'id': self.id });
    self.id = opts.id;

    self.stateMachine = opts.stateMachine;
    self.clog = opts.clog;
    self.updateCluster();

    self.snapshotter = opts.snapshotter;
    self.snapshotter.setRaft(self);

    self.messageBus = opts.messageBus;
    self.messageBus.register(self);
    self.outstandingMessages = {};

    self.properties = opts.properties;

    //This is set in the resetTicker function.  The ticker is used to transition
    // to a candidate (if follower or candidate), or send heartbeats to
    // followers (if leader).
    self.leaderTimeout = undefined;
    self.leaderId = undefined;
    self.state = undefined;
    self.transitioning = false;

    //These are only needed when this is a leader
    self.peerMatchIndexes = undefined;
    self.peerNextIndexes = undefined;

    //Pipes for request processing...
    self.requestVotesPipe = getRequestVotesPipe.call(self);
    self.appendEntriesPipe = getAppendEntriesPipe.call(self);
    self.executeEntriesPipe = getExecuteEntriesPipe.call(self);
    self.installSnapshotPipe = getInstallSnapshotPipe.call(self);

    self.resetTicker();
    self.transitionToFollower();
}

util.inherits(Raft, events.EventEmitter);
module.exports = Raft;



///--- Internal classes

/**
 * Filters the stream for the state machine, so that the state machine only has
 * to look for 'noop'.
 */
function NoopEntriesTransform() {
    stream.Transform.call(this, { 'objectMode': true });
}
util.inherits(NoopEntriesTransform, stream.Transform);

NoopEntriesTransform.prototype._transform = function (entry, encoding, cb) {
    if ((typeof (entry)) === 'object') {
        if (entry.to !== undefined && entry.to === 'raft') {
            entry = 'noop';
        }
    }
    this.push(entry);
    cb();
};



///--- Request Vote

//Serialize all request vote requests.
function getRequestVotesPipe() {
    var self = this;
    var rvp = new TaskPipe({
        'tasks': [ {
            'name': '_requestVote',
            'func': _requestVote.bind(self)
        } ]
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

    //I don't know who you are....
    if (!self.cluster.peerExists(req.candidateId)) {
        log.error({ 'candidateId': req.candidateId,
                    'cluster': self.cluster },
                  'request vote from unknown peer');
        return (cb(new error.InvalidPeerError(sprintf(
            'unknown peer %s', req.candidateId)), voteGranted));
    }

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

    //We don't vote for candidates of they aren't eligable in all configs
    if (!self.cluster.votingInAllConfigs(req.candidateId)) {
        log.debug({ 'candidateId': req.candidateId,
                    'votedFor': votedFor },
                  'candidate isnt voting status in all configs');
        return (cb(null, voteGranted));
    }

    //If we aren't allowed to vote, don't.
    if (!self.cluster.votingInLatestConfig(self.id)) {
        log.debug({ 'candidateId': req.candidateId,
                    'votedFor': votedFor },
                  'i\'m not allowed to vote');
        return (cb(null, voteGranted));
    }

    //Already voted for this leader, this term.
    if (votedFor === req.candidateId) {
        log.debug({ 'candidateId': req.candidateId,
                    'votedFor': votedFor },
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
    var vtas = new TaskPipe({
        'tasks': [ {
            'name': '_verifyTermAndStatus',
            'func': _verifyTermAndStatus.bind(self)
        }, {
            'name': '_appendToCLog',
            'func': _appendToCLog.bind(self)
        }, {
            'name': '_updateClusterConfig',
            'func': _updateClusterConfig.bind(self)
        }, {
            'name': '_executeLatestEntries',
            'func': _executeLatestEntries.bind(self)
        } ]
    });

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

    function checkParams(_, subcb) {
        //I haven't been bootstrapped yet...
        if (self.cluster.clogIndex === -1) {
            log.debug({
                'cluster': self.cluster
            }, 'not bootstrapped yet.');
            return (cb(new error.NotBootstrappedError('not bootstrapped yet')));
        }

        //I don't know who you are....
        if (!self.cluster.peerExists(req.leaderId)) {
            log.error({ 'leaderId': req.leaderId,
                        'cluster': self.cluster },
                      'append entries from unknown peer');
            return (cb(new error.InvalidPeerError(sprintf(
                'unknown peer %s', req.leaderId))));
        }

        return (subcb());
    }

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
            log.error({
                'term': req.term,
                'reqLeader': req.leaderId,
                'selfLeader': self.leaderId
            }, 'possible leader conflict!!!!');
            return (subcb(new error.InternalError('leader mismatch')));
        }
        if (req.term !== currentTerm) {
            log.debug({ 'currentTerm': currentTerm,
                        'requestTerm': req.term },
                      'My term is out of date.');
            currentTerm = req.term;
            self.updateTermAndVotedFor(req.term, function () {
                //We know the leader from the appendEntries request.
                self.leaderId = req.leaderId;
                return (subcb(null));
            });
        } else {
            //Just take the first that shows up for the term.
            if (self.leaderId === undefined) {
                self.leaderId = req.leaderId;
            }
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
            checkParams,
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
    self.clog.append({
        'commitIndex': req.commitIndex,
        'term': req.term,
        'entries': req.entries
    }, function (err) {
        if (err) {
            return (cb(err));
        }
        cb(null);
    });
}


//This handles cluster reconfiguration on followers.
function _updateClusterConfig(req, cb) {
    assert.arrayOfObject(req, 'req');
    assert.ok(req.length === 1);
    req = req[0];

    var self = this;
    //Check if the last set of appends
    if (self.cluster.clogIndex === self.clog.clusterConfig.clogIndex) {
        setImmediate(cb);
        return;
    }

    //We have a cluster reconfiguration to process.
    self.updateCluster();
    setImmediate(cb);
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



///--- Execute Entries

function getExecuteEntriesPipe() {
    var self = this;
    var ee = new TaskPipe({
        'tasks': [ {
            'name': '_executeEntries',
            'func': _executeEntries.bind(self)
        } ]
    });
    return (ee);
}


/**
 * This is the actual implementation of execute entries
 */
function _executeEntries(index, cb) {
    assert.arrayOfNumber(index, 'index');
    assert.ok(index.length === 1);
    index = index[0];

    var self = this;
    var log = self.log;
    var message;

    if (index >= self.clog.nextIndex) {
        message = 'attempted to execute command past end of clog';
        log.error({
            'index': index,
            'clogNextIndex': self.clog.nextIndex
        }, message);
        return (setImmediate(cb.bind(null, new Error(message))));
    }

    //Entries are executed in the state machine when:
    //  1) A follower receives the commitIndex from the leader
    //  2) A leader has one entry committed from current term
    // In case #1, this function should blindly trust the index and execute up
    // to that point (if it hasn't already).  In case #2, the leader should only
    // be sending the index after a client request is processed, so it *can* be
    // blindly trusted.  But since we're keeping track of the highest known log
    // for each follower, we can do an independent verification.
    if (self.state === STATES.leader) {
        //The full rule for committing is that a leader must have at least one
        // entry from its own term (guaranteed by the way this function is only
        // called during a client request) and by the log entry being stored on
        // a majority of servers.
        var peerMatched = Object.keys(self.peerMatchIndexes).map(function (i) {
            return (self.peerMatchIndexes[i]);
        });
        peerMatched.push(self.clog.nextIndex - 1);
        peerMatched.sort(function (a, b) {
            //All undefined vals should go to the end.
            return (b - a);
        });
        var nIndex = peerMatched[Math.floor(peerMatched.length / 2)];
        log.debug({ 'index': index,
                    'logIndex': self.clog.nextIndex - 1,
                    'peers': self.peerMatchIndexes,
                    'sortedPeerMatches': peerMatched,
                    'nIndex': nIndex
                  }, 'decided on new commit index');
        if (index > nIndex) {
            message = 'attemted to apply a log entry to the state ' +
                'machine on the master when its not time yet.';
            return (setImmediate(cb.bind(null, new Error(message))));
        }
        //Execute up to what we can, thank you very much.
        index = nIndex;
    }

    //Kick out if the index has already been executed.
    var smIndex = self.stateMachine.commitIndex;
    if (index < smIndex) {
        return (setImmediate(cb));
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

        //We send a filtered stream to the state machine so that all raft
        // operations become a 'noop'.
        var noopTransform = new NoopEntriesTransform();
        entries.pipe(noopTransform);
        entries = noopTransform;
        self.stateMachine.execute(entries, function (err2) {
            if (err2) {
                log.error({ 'err': err2, 'entries': entries },
                          'error executing entries on state machine');
            }
            return (cb(err2));
        });
    });
}


///--- Install Snapshot

//Serialize all install snapshot requests.
function getInstallSnapshotPipe() {
    var self = this;
    var isp = new TaskPipe({
        'tasks': [ {
            'name': '_installSnapshot',
            'func': _installSnapshot.bind(self)
        } ]
    });
    return (isp);
}


function _installSnapshot(req, cb) {
    assert.arrayOfObject(req, 'index');
    assert.ok(req.length === 1);
    req = req[0];
    assert.object(req, 'req');
    assert.object(req.snapshot, 'req.snapshot');
    assert.func(cb, 'cb');

    var self = this;
    var log = self.log;

    log.debug('installing snapshot');

    //TODO: Lock ourselves and wait for the pipes to drain...
    self.snapshotter.read(req.snapshot, function (err, snapshot) {
        if (err) {
            return (cb(err));
        }

        //If we successfully read and applied the snaphot, we go ahead and
        // reset ourself.

        //Reading a snapshot doesn't mean we know who the current leader is.  We
        // do need to keep around whick server we last voted for.

        // The only things that the snaphotter should put in place is (see
        // section 7 of the raft paper):
        // 1. Command log, including the commit index
        // 2. State Machine
        // 3. Latest cluster configuration
        self.stateMachine = snapshot.stateMachine;
        self.clog = snapshot.clog;
        self.updateCluster();

        self.leaderId = undefined;
        self.transitioning = false;

        self.resetTicker();
        self.transitionToFollower();

        log.debug('done installing snapshot');
        return (cb());
    });
}



///--- Helpers

/**
 * Handles all the client messages that are intended for the raft cluster.
 * These are all valid raft-specific messages.
 *
 * { "to": "raft", "execute": "noop" }
 *
 * --- Cluster reconfigurations: ---
 * # Add a new peer, will be in non-voting status until autoPromote or it is
 * # manually promoted.
 * { "to": "raft", "execute": "addPeer", "id": "[id]", "autoPromote": [bool],
 *   "config": { "connection": ... } }
 * # Manually promote a peer to voting status.  Should fail if the peer hasn't
 * # ever been served an "appendEntries" call since this raft instance was
 * # promoted to be a leader.
 * { "to": "raft", "execute": "promotePeer", "id": "[id]" }
 * # Manually demote a peer to non-voting status.
 * { "to": "raft", "execute": "demotePeer", "id": "[id]" }
 * # Removes a peer, regardless of status.
 * { "to": "raft", "execute": "removePeer", "id": "[id]" }
 *
 * --- Manually mess with this raft instance ---
 * # Force a step up, only if it is a follower.
 * { "to": "raft", "execute": "stepUp" }
 * # Force a step down, only if it is a leader or candidate.
 * { "to": "raft", "execute": "stepDown" }
 */
function _handleRaftRequest(command, cb) {
    assert.object(command, 'command');
    assert.func(cb, 'cb');

    var self = this;
    command.execute = command.execute === undefined ? '<undefined>' :
        command.execute;

    switch (command.execute) {
    //We'll handle cluster reconfigurations later.
    case 'addPeer':
    case 'promotePeer':
    case 'demotePeer':
    case 'removePeer':
        break;
    case 'stepUp':
        if (self.state === STATES.follower) {
            self.log('manual request to step up to candidate');
            self.transitionToCandidate();
        }
        return (setImmediate(cb));
    case 'stepDown':
        if (self.state !== STATES.follower) {
            self.log('manual request to step down to follower');
            self.transitionToFollower();
        }
        return (setImmediate(cb));
    case 'noop':
        return (setImmediate(cb));
    default:
        return (setImmediate(cb.bind(null, new error.InvalidRaftRequestError(
            sprintf('execute %s is an invalid command', command.execute)))));
    }

    //Now we assume that this is a cluster configuration
    if (self.cluster.reconfiguration) {
        return (setImmediate(cb.bind(null, new error.InvalidRaftRequestError(
            'cluster is already being reconfigured'))));
    }
    if (command.id === undefined) {
        return (setImmediate(cb.bind(null, new error.InvalidRaftRequestError(
            'no raft id specified'))));
    }

    var peer;
    var newClusterConfig = deepcopy(self.cluster.clusterConfig.current);
    switch (command.execute) {
    case 'addPeer':
        if (self.cluster.exists(command.id)) {
            return (setImmediate(cb));
        }
        command.config = command.config || {};
        command.config.voting = false;
        newClusterConfig[command.id] = command.config;
        break;
    case 'promotePeer':
        if (!self.cluster.exists(command.id)) {
            return (setImmediate(cb.bind(
                null, new error.InvalidRaftRequestError('unknown raft id'))));
        }
        peer = self.cluster.get(command.id);
        if (peer.voting) {
            return (setImmediate(cb));
        }
        newClusterConfig[command.id].voting = true;
        break;
    case 'demotePeer':
        if (!self.cluster.exists(command.id)) {
            return (setImmediate(cb.bind(
                null, new error.InvalidRaftRequestError('unknown raft id'))));
        }
        peer = self.cluster.get(command.id);
        if (!peer.voting) {
            return (setImmediate(cb));
        }
        newClusterConfig[command.id].voting = false;
        break;
    case 'removePeer':
        if (!self.cluster.exists(command.id)) {
            return (setImmediate(cb));
        }
        delete newClusterConfig[command.id];
        break;
    default:
        self.log.error({
            'command': command
        }, 'somehow got here?!');
        return (setImmediate(cb.bind(
            null, new error.InternalError('somehow got here'))));
    }

    //TODO:
    // Handling failed leader during reconfiguration and autopromotion

    var changeCommand = {
        'to': 'raft',
        'execute': 'configure',
        'cluster': {
            'old': self.clog.clusterConfig.current,
            'new': newClusterConfig
        }
    };


    //Now send it through clientRequest (yes, there would be a feedback loop
    // here if we didn't look for the 'execute': 'configure' specifically.
    self.clientRequest({ 'command': changeCommand }, function (err) {
        if (err) {
            return (cb(err));
        }
        changeCommand = {
            'to': 'raft',
            'execute': 'configure',
            'cluster': {
                'current': newClusterConfig
            }
        };
        self.clientRequest({ 'command': changeCommand }, function (err2) {
            if (err2) {
                return (cb(err2));
            }

            //Leader steps down if not part of the cluster or is no longer a
            // voting member.
            if (self.state === STATES.leader &&
                !self.cluster.votingInLatestConfig(self.id)) {
                self.log.debug({
                    'cluster': self.cluster
                }, 'leader isn\'t part of new cluster, stepping down');
                self.transitionToFollower();
            }

            //Autopromote - isn't going to work if this leader isn't part of the
            // new config.  Also, we should only autopromote after it has
            // finished catching up with the rest of the cluster.  So this works
            // for now, but we'll need to fix it.
            if (command.execute === 'addPeer' &&
                command.autoPromote &&
                self.cluster.votingIds.indexOf(command.id) === -1) {
                var promoteCommand = {
                    'to': 'raft',
                    'execute': 'promotePeer',
                    'id': command.id
                };
                self.log.debug(promoteCommand, 'autopromoting');
                _handleRaftRequest.call(self, promoteCommand, cb);
            } else {
                cb();
            }
        });
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
        return (setImmediate(cb));
    }
    //If we don't need to update... don't
    if (oldTerm === term && oldVotedFor === votedFor) {
        return (setImmediate(cb));
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


Raft.prototype.updateCluster = function () {
    var self = this;

    self.log.debug({
        'oldCluster': self.cluster,
        'newClusterConfig': self.clog.clusterConfig
    }, 'adopting new cluster config');

    if (self.clog.clusterConfig === undefined) {
        self.log.debug({
            'clusterConfig': self.clog.clusterConfig
        }, 'clog cluster isn\'t defined, leaving empty cluster');

        //This is an "empty" cluster config.  Not sure if I like the -1
        // clogIndex.
        self.cluster = new Cluster({
            'id': self.id,
            'clusterConfig': { 'current': {}, 'clogIndex': -1 }
        });
        return;
    }

    self.cluster = new Cluster({
        'id': self.id,
        'clusterConfig': self.clog.clusterConfig
    });

    //Need to make sure we have the right peers in the peer next indexes.  The
    // peer match index takes care of itself.
    if (self.state === STATES.leader) {
        var last = self.clog.last();
        var nextIndex = last.index + 1;
        self.cluster.allPeerIds.forEach(function (peer) {
            if (self.peerNextIndexes[peer] === undefined) {
                self.peerNextIndexes[peer] = nextIndex;
            }
        });
    }

    self.log.debug({
        'cluster': self.cluster
    }, 'new cluster');
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
    assert.object(req.entries, 'request.entries');
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


Raft.prototype.executeLatestEntries = function (index, cb) {
    assert.number(index, 'index');
    assert.func(cb, 'cb');

    var self = this;
    var log = self.log;

    log.debug({ 'index': index }, 'Entering executeLatestEntries');

    self.executeEntriesPipe.append(index, function (err) {
        log.debug({ 'err': err }, 'Leaving executeLatestEntries');
        return (cb(err));
    });
};


Raft.prototype.installSnapshot = function (req, cb) {
    assert.object(req, 'req');
    assert.object(req.snapshot, 'req.snapshot');
    assert.func(cb, 'cb');

    var self = this;
    var log = self.log;

    log.debug('Entering installSnapshot');

    self.installSnapshotPipe.append(req, function (err) {
        log.debug({ 'err': err }, 'Leaving installSnapshot');
        return (cb(err));
    });
};


//TODO: This is (eventually) the thing we need to be as fast as possible.
/**
 * The req.command can be any serializable objects (serializable in the
 * JSON.serialize sense of the word).  There are a few special way of addressing
 * raft, for some very specific things:
 *
 * The string 'noop' is a no-operation raft command.  The state machine
 * will ignore this, but it will still be in the logs.
 *
 * Any object with the the field "to": "raft" will be ignored by the state
 * machine and is destined for raft-specific commands.  For example, this
 * is the 'stepDown' command, that forces this, if it is a leader, to step
 * down:
 *
 * { "to": "raft", "execute": "stepDown" }
 *
 * You can find a list of all the raft-specific messaged with the
 * _handleRaftRequest function.
 */
Raft.prototype.clientRequest = function (req, cb) {
    assert.object(req, 'req');
    assert.func(cb, 'cb');
    //We just make sure it's not null.
    assert.ok(req.command, 'req.command');

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
        var o = {
            'commitIndex': -1,
            'term': -1,
            'entries': memstream([ entry ])
        };
        self.clog.append(o, function (err, res) {
            if (err) {
                return (subcb(err));
            }
            entry.index = res.index;
            return (subcb(null));
        });
    }

    //This handles cluster reconfig on the leader.
    function clusterConfigUpdate(_, subcb) {
        if (self.cluster.clogIndex === self.clog.clusterConfig.clogIndex) {
            setImmediate(subcb);
            return;
        }
        self.updateCluster();
        setImmediate(subcb);
    }

    function appendEntry(_, subcb) {
        self.sendAppendEntries(entry.index, subcb);
    }

    function executeEntry(_, subcb) {
        self.executeLatestEntries(entry.index, subcb);
    }

    function handleAppend(_, subcb) {
        vasync.pipeline({
            'funcs': [
                appendToCLog,
                clusterConfigUpdate,
                appendEntry,
                executeEntry
            ]
        }, subcb);
    }

    function splitPipeline(_, subcb) {
        //Let cluster configs drop through, all other raft requests should go
        // through the _handleRaftRequest function.
        if (req.command.to && req.command.to === 'raft' &&
            req.command.execute !== 'configure') {
            _handleRaftRequest.call(self, req.command, subcb);
        } else {
            handleAppend(_, subcb);
        }
    }

    vasync.pipeline({
        funcs: [
            verifyImLeader,
            splitPipeline
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


//This does the dumb thing for now.  It verifies index is in current range, then
// sends as much as it can each time.  We may want to revisit so that we have
// only a certain amount of requests outstanding, or we choke them but since
// requests are idempotent, the above is, technically, correct, but there are
// drawbacks:
// 1) Lots of unnecessary network traffic
// 2) Causes lots of checking on the followers
// 3) If a peer is down, there are going to be a lot of messages in the queue
//    for when (if) that peer wakes up.
Raft.prototype.sendAppendEntries = function (index, cb) {
    if ((typeof (index)) === 'function') {
        cb = index;
        index = undefined;
    }

    var self = this;
    var log = self.log;

    //Short-circuit if there are no peers
    if (self.cluster.numPeers < 1) {
        log.debug('send append entries with no peers.  short-circuiting.');
        self.resetTicker();
        return (setImmediate(cb));
    }

    //For the duration of these calls, we'll take what is currently the end.
    var clogEnd = self.clog.nextIndex;

    //This is the only thing we do with the index: verify that the requester
    // has actually got an index that is in the log.
    if (index !== undefined && index >= clogEnd) {
        var message = 'tried to append entries with an index that is too large';
        log.error({
            'index': index,
            'clogEnd': clogEnd
        }, message);
        return (setImmediate(cb.bind(null, new Error(message))));
    }

    log.debug('sending appendEntries');

    function getAppendEntries(peer, subcb) {
        var peerIndex = self.peerNextIndexes[peer];
        //The "-1" is for the consistency check entry.
        //We're taking everything for now, which isn't good behavior if a peer
        // is down for an extended period of time (or gets slowly out of date)
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

    var replied = [ self.id ];
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
            --self.peerNextIndexes[from];
            getAppendEntries(from, function (err2, appendRequest) {
                //TODO: err2?
                self.send(from, appendRequest, onResponse);
            });
            return;
        }

        if (res.success) {
            //Set the next index.
            if (clogEnd > self.peerNextIndexes[from]) {
                self.peerNextIndexes[from] = clogEnd;
            }
            //Set now we know how up to date the peer is.
            if (self.peerMatchIndexes[from] === undefined ||
                clogEnd - 1 > self.peerMatchIndexes[from]) {
                self.peerMatchIndexes[from] = clogEnd - 1;
            }
        }

        if (replied.indexOf(from) === -1) {
            replied.push(from);
        }
        //We are using the latest cluster for the majority, not the majority at
        // the time the request started.  TODO: Not sure if that's the correct
        // thing to do.
        if (self.cluster.isMajority(replied)) {
            self.resetTicker();
            end();
        }
    }

    self.cluster.allPeerIds.forEach(function (peer) {
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
    self.peerNextIndexes = {};
    self.cluster.allPeerIds.forEach(function (peer) {
        self.peerNextIndexes[peer] = nextIndex;
    });
    self.peerMatchIndexes = {};

    //TODO: See the docs, but only doing this, then accepting client requests
    // will lead to an eventually consistent client read.  It needs to be fixed
    // by writing a noop and waiting for that write and the state machine
    // executions to go through.
    self.sendAppendEntries(function (err) {
        if (err) {
            log.error({
                'error': err
            }, 'append entries on leader transition failed');
        } else {
            log.debug('append entries from transition to master completed');
        }
    });
    self.leaderId = self.id;
    self.state = STATES.leader;
    setImmediate(function () {
        self.emit('stateChange', self.state);
    });
};


Raft.prototype.transitionToCandidate = function () {
    var self = this;
    var log = self.log;
    log.debug('transitioning to candidate');
    self.clearOutstandingMessages();
    self.state = STATES.candidate;

    //TODO: If I'm already transitioning, just ignore more requests until I've
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

                    if (self.cluster.isMajority(votesFrom)) {
                        log.debug({
                            'votesFrom': votesFrom,
                            'cluster': self.cluster
                        }, 'recieved majority vote');
                        self.transitionToLeader();
                        receivedMajority = true;
                    }
                }

                if (self.cluster.numPeers > 0) {
                    log.debug({
                        'votingPeerIds': self.cluster.votingPeerIds
                    }, 'sending request votes to peers');
                    self.cluster.votingPeerIds.forEach(function (peer) {
                        self.send(peer, requestVote, onResponse);
                    });
                } else {
                    log.debug({
                        'cluster': self.cluster
                    }, 'no peers, transitioning straight to leader');
                    receivedMajority = true;
                    setImmediate(function () {
                        self.transitionToLeader();
                    });
                }

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
    setImmediate(function () {
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
    if (self.state === STATES.leader) {
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
    var log = self.log;

    //We don't tick if we are non-voting in any of the cluster configs
    if (!self.cluster.votingInAllConfigs(self.id)) {
        self.log.debug('read-only.  not ticking.');
        self.leaderTimeout = TICK_MAX;
        return;
    }

    --self.leaderTimeout;
    self.log.debug({ 'state': self.state,
                     'leaderTimeout': self.leaderTimeout },
                   'ticking');
    if (self.leaderTimeout <= 0) {
        if (self.state === STATES.candidate || self.state === STATES.follower) {
            self.log.debug('leader timed out, transitioning to candidate');
            self.transitionToCandidate();
        } else {
            self.sendAppendEntries(function (err) {
                log.debug({ 'error': err }, 'append entries tick complete');
            });
        }
    }
};


Raft.prototype.summary = function () {
    var self = this;
    return ({
        id: self.id,
        currentTerm: self.currentTerm(),
        state: self.state,
        cluster: self.cluster,
        clog: {
            clog: self.clog.clog.map(function (o) { return (o.command); }),
            nextIndex: self.clog.nextIndex,
            clusterConfig: self.clog.clusterConfig
        },
        stateMachine: {
            commitIndex: self.stateMachine.commitIndex,
            data: self.stateMachine.data
        },
        messageBus: self.messageBus.messages,
        outstandingMessages: self.outstandingMessages,
        votedFor: self.votedFor(),
        leaderTimeout: self.leaderTimeout,
        leaderId: self.leaderId
    });
};
