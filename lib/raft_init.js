/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var deepcopy = require('deepcopy');
var defaults = require('./defaults');
var lib = require('./leveldb');
var MessageBus = require('./message_bus');
var Raft = require('./raft');
var vasync = require('vasync');



/**
 * Init Raft from a config block.  For example:
 * {
 *     "id": "foo.vera.example.com",  //Raft Id
 *     "location": "/etc/vera/data",  //Data location (leveldb)
 *     "isFirstPeer": true,           //Optional.  Only do this the *first time*
 *                                    // you spin up the cluster.
 *     "clusterConfig": {             //Only applies if ^^ is true.
 *         "foo.vera.example.com": {  //Raft Id
 *             "ip": "10.99.99.19",   //IP Address
 *             "port": 1919,          //Port, optional, defaults to 1919
 *             "voting": true         //Is a voting member, defaults to true
 *         }
 *     }
 * }
 *
 * A note about the clusterConfig... If you look at the actual cluster config
 * object (see ./cluster.js), the object above is a simple transformation:
 * 1. Iterate over all peers and make sure "voting": [true|false], will default
 *    it to true if the property isn't present.
 * 2. clusterConfig = { "current": <the object above> }
 */

///--- Functions

function createClusterConfig(clusterConfig) {
    assert.object(clusterConfig, 'clusterConfig');

    var c = deepcopy(clusterConfig);
    var peers = Object.keys(c);
    var seenPeers = [];
    if (peers.length < 1) {
        throw new Error('cluster config is empty');
    }
    peers.forEach(function (peer) {
        if (seenPeers.indexOf(peer) !== -1) {
            throw new Error('duplicate peers in cluster config');
        }
        seenPeers.push(peer);
        var p = c[peer];
        assert.string(p.ip, 'peer.ip');
        if (p.port === undefined) {
            p.port = defaults.PORT;
        }
        if (p.voting === undefined) {
            p.voting = true;
        }
    });
    return ({ 'current': c });
}


function from(opts, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.id, 'opts.id');
    assert.string(opts.location, 'opts.location');
    assert.optionalBool(opts.isFirstPeer, 'opts.isFirstPeer');
    if (opts.isFirstPeer === true) {
        assert.object(opts.clusterConfig, 'opts.clusterConfig');
    }
    assert.func(cb, 'cb');

    var log = opts.log;
    var location = opts.location;

    var r;
    vasync.pipeline({
        arg: opts,
        funcs: [
            function initOrOpenLevelDb(_, subcb) {
                lib.dbHelpers.createOrOpen({
                    'log': log,
                    'location': location
                }, function (err, res) {
                    if (err) {
                        return (subcb(err));
                    }
                    _.db = res.db;
                    subcb();
                });
            },
            function initMessageBus(_, subcb) {
                _.messageBus = new MessageBus({ 'log': log });
                _.messageBus.on('ready', subcb);
            },
            function initStateMachine(_, subcb) {
                _.stateMachine = new lib.StateMachine({
                    'log': log,
                    'db': _.db
                });
                _.stateMachine.on('ready', subcb);
            },
            function initCommandLog(_, subcb) {
                var cOpts = {
                    'log': log,
                    'db': _.db,
                    'stateMachine': _.stateMachine
                };
                //Only pay attention to the cluster config if isFirstPeer
                if (opts.isFirstPeer === true) {
                    cOpts.clusterConfig = createClusterConfig(
                        opts.clusterConfig);
                }
                _.clog = new lib.CommandLog(cOpts);
                _.clog.on('ready', subcb);
                _.clog.on('error', subcb);
            },
            function initProperties(_, subcb) {
                _.properties = new lib.Properties({
                    'log': log,
                    'db': _.db
                });
                _.properties.on('ready', subcb);
            },
            function initLevelDbSnapshotter(_, subcb) {
                _.snapshotter = new lib.Snapshotter({
                    'log': log,
                    'db': _.db
                });
                _.snapshotter.on('ready', subcb);
            },
            function initRaft(_, subcb) {
                r = new Raft(opts);
                subcb();
            }
        ]
    }, function (err) {
        if (err) {
            return (cb(err));
        }

        return (cb(null, r));
    });
}


module.exports = {
    'from': from
};
