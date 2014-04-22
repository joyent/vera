// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var lib = require('./leveldb');
var MessageBus = require('./message_bus');
var Raft = require('./raft');
var vasync = require('vasync');



/**
 * Init Raft from a config block.
 */

///--- Functions

function from(opts, cb) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.id, 'opts.id');
    assert.string(opts.location, 'opts.location');
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
                _.clog = new lib.CommandLog({
                    'log': log,
                    'db': _.db,
                    'stateMachine': _.stateMachine
                });
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
