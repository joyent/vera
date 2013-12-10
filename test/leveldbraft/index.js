// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var fs = require('fs');
var helper = require('../helper.js');
var LevelDbLog = require('../../lib/leveldb/log');
//TODO: Should be persisted.
var MemProps = require('../memraft/memprops');
//TODO: Move the message bus out somewhere?
var MessageBus = require('../memraft/messagebus');
var path = require('path');
var Raft = require('../../lib/raft');
//TODO: Should be persisted.
var StateMachine = require('../memraft/statemachine');
var vasync = require('vasync');



///--- Globals
var TMP_DIR = path.resolve(path.dirname(__dirname), '..') + '/tmp';
var DB_FILE = TMP_DIR + '/leveldb_log_test.db';



///--- Funcs

function raft(opts, cb) {
    assert.object(opts);
    assert.object(opts.log, 'opts.log');
    assert.string(opts.id, 'opts.id');
    assert.arrayOfString(opts.peers, 'opts.peers');
    assert.optionalObject(opts.messageBus, 'opts.messageBus');
    assert.optionalString(opts.dbName, 'opts.dbName');

    var dbName = opts.dbName || 'unknown_test_db';
    var dbLocation = TMP_DIR + '/' + dbName;

    var log = opts.log;

    var r;
    vasync.pipeline({
        arg: opts,
        funcs: [
            function mkTmpDir(_, subcb) {
                fs.mkdir(TMP_DIR, function (err) {
                    if (err && err.code !== 'EEXIST') {
                        return (subcb(err));
                    }
                    return (subcb());
                });
            },
            function removeOldLevelDb(_, subcb) {
                helper.rmrf(dbLocation, subcb);
            },
            function initMessageBus(_, subcb) {
                if (_.messageBus === undefined) {
                    _.messageBus = new MessageBus({ 'log': log });
                    _.messageBus.on('ready', subcb);
                } else {
                    subcb();
                }
            },
            function initStateMachine(_, subcb) {
                _.stateMachine = new StateMachine({ 'log': log });
                _.stateMachine.on('ready', subcb);
            },
            function initDb(_, subcb) {
                _.clog = new LevelDbLog({
                    'log': log,
                    'location': dbLocation,
                    'stateMachine': _.stateMachine
                });
                _.clog.on('ready', subcb);
                _.clog.on('error', subcb);
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



///--- Exports
module.exports = {
    'raft': raft
};
