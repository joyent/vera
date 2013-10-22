// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var MemLog = require('./memlog');
var MemProps = require('./memprops');
var MessageBus = require('./messagebus');
var Raft = require('../../lib/raft');
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

//Inits a set of raft peers, all connected with a single message bus.
function cluster(opts, cb) {
    assert.object(opts);
    assert.object(opts.log, 'opts.log');
    assert.number(opts.size, 'opts.size');

    //Only putting everything in here so you can see what will be returned.
    var c = {
        'messageBus': undefined,
        'peers': {}
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
