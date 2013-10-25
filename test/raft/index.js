// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var MemLog = require('./memlog');
var MemProps = require('./memprops');
var MessageBus = require('./messagebus');
var Raft = require('../../lib/raft');
var sprintf = require('extsprintf').sprintf;
var StateMachine = require('./statemachine');
var vasync = require('vasync');



///--- Funcs

function memRaft(opts, cb) {
    assert.object(opts);
    assert.object(opts.log, 'opts.log');
    assert.string(opts.id, 'opts.id');
    assert.arrayOfString(opts.peers, 'opts.peers');
    assert.object(opts.messageBus, 'opts.messageBus');

    var log = opts.log;

    var raft;
    vasync.pipeline({
        arg: opts,
        funcs: [
            function initStateMachine(_, subcb) {
                _.stateMachine = new StateMachine({ 'log': log });
                _.stateMachine.on('ready', subcb);
            },
            function initMemLog(_, subcb) {
                _.clog = new MemLog({ 'log': log });
                _.clog.on('ready', subcb);
            },
            function initMemProps(_, subcb) {
                _.properties = new MemProps({
                    'log': log,
                    'props': {
                        'currentTerm': 0
                    }
                });
                _.properties.on('ready', subcb);
            },
            function initRaft(_, subcb) {
                raft = new Raft(opts);
                subcb();
            }
        ]
    }, function (err) {
        if (err) {
            return (cb(err));
        }
        return (cb(null, raft));
    });
}


function clusterToString(c) {
    var self = c || this;
    var s = '';

    //Peers
    Object.keys(self.peers).map(function (p) {
        var peer = self.peers[p];
        s += sprintf(
            '%s %9s term: %3d, t-out: %2d, leader: %s, rvp: %d\n',
            peer.id, peer.state, peer.currentTerm(), peer.leaderTimeout,
            peer.leaderId === undefined ? 'undefd' : peer.leaderId,
            peer.requestVotesPipe.inProgress ? '1' : '0');
    });

    //Messages
    s += sprintf(
        'Messages:%s\n',
        Object.keys(self.messageBus.messages).length === 0 ? ' (none)' : '');
    Object.keys(self.messageBus.messages).forEach(function (mi) {
        var m = self.messageBus.messages[mi];
        var prefix = sprintf('%s -> %s: term %3d', m.from, m.to,
                             m.message.term);
        if (m.message.operation === 'requestVote') {
            s += sprintf('  reqVote %s, logIndex: %3d, logTerm: %3d\n', prefix,
                         m.message.lastLogIndex, m.message.lastLogTerm);
        } else {
            s += sprintf('  appEntr %s, leader: %s, commitIndex: %3d\n', prefix,
                         m.message.leaderId, m.message.commitIndex);
        }
    });

    return (s);
}


//Inits a set of raft peers, all connected with a single message bus.
function cluster(opts, cb) {
    assert.object(opts);
    assert.object(opts.log, 'opts.log');
    assert.number(opts.size, 'opts.size');
    assert.optionalBool(opts.electLeader, 'opts.electLeader');

    //Only putting everything in here so you can see what will be returned.
    var c = {
        'messageBus': undefined,
        'peers': {},
        'toString': clusterToString
    };
    var log = opts.log;
    var peers = [];
    for (var i = 0; i < opts.size; ++i) {
        peers.push('raft-' + i);
    }
    vasync.pipeline({
        arg: {},
        funcs: [
            function initMessageBus(_, subcb) {
                c.messageBus = new MessageBus({ 'log': log });
                c.messageBus.on('ready', subcb);
            },
            function initPeers(_, subcb) {
                var inited = 0;
                function tryEnd() {
                    ++inited;
                    if (inited === opts.size) {
                        subcb();
                    }
                }

                peers.forEach(function (p) {
                    var o = {
                        'log': log,
                        'id': p,
                        'peers': peers,
                        'messageBus': c.messageBus
                    };
                    memRaft(o, function (err, peer) {
                        c.peers[p] = peer;
                        tryEnd();
                    });
                });
            },
            function electLeader(_, subcb) {
                if (opts.electLeader === undefined) {
                    return (subcb());
                }

                var x = 0;
                var s = '';
                function tryOnce() {

                    //Error out if this is taking "too long"
                    s += sprintf('%d:\n%s\n', x, c.toString());
                    if (x++ === 40) {
                        console.error(s);
                        return (subcb(
                            new Error('leader election took too long')));
                    }

                    //End condition is when there is one leader and the rest
                    // followers.
                    var states = { 'leader': 0, 'follower': 0, 'candidate': 0 };
                    Object.keys(c.peers).forEach(function (p) {
                        ++states[c.peers[p].state];
                    });
                    if (states['leader'] === 1 &&
                        states['follower'] === peers.length - 1) {
                        return (subcb());
                    }

                    //Otherwise, move the cluster along...
                    if (Object.keys(c.messageBus.messages).length > 0) {
                        c.messageBus.tick(function () {
                            return (process.nextTick(tryOnce));
                        });
                    } else {
                        Object.keys(c.peers).forEach(function (p) {
                            c.peers[p].tick();
                        });
                        return (process.nextTick(tryOnce));
                    }
                }
                tryOnce();
            }
        ]
    }, function (err) {
        if (err) {
            return (cb(err));
        }
        cb(null, c);
    });
}



///--- Exports
module.exports = {
    'cluster': cluster
};
