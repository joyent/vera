// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var fs = require('fs');
var helper = require('../helper.js');
var LevelDbLog = require('../../lib/leveldb/log');
var levelDbIndex = require('../../lib/leveldb');
var LevelDbProperties = require('../../lib/leveldb/properties');
var MessageBus = require('../messagebus');
var path = require('path');
var Raft = require('../../lib/raft');
var StateMachine = require('./statemachine');
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
            function initLevelDb(_, subcb) {
                levelDbIndex.createOrOpen({
                    'log': log,
                    'location': dbLocation
                }, function (err, res) {
                    if (err) {
                        return (subcb(err));
                    }
                    _.db = res.db;
                    subcb();
                });
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
                _.stateMachine = new StateMachine({
                    'log': log,
                    'db': _.db
                });
                _.stateMachine.on('ready', subcb);
            },
            function initCommandLog(_, subcb) {
                _.clog = new LevelDbLog({
                    'log': log,
                    'db': _.db,
                    'stateMachine': _.stateMachine
                });
                _.clog.on('ready', subcb);
                _.clog.on('error', subcb);
            },
            function initLevelDbPropertiesProps(_, subcb) {
                _.properties = new LevelDbProperties({
                    'log': log,
                    'db': _.db
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
