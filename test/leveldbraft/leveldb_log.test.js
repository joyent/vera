// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('../helper.js');
var LevelDbLog = require('../../lib/leveldb/log');
var path = require('path');
var StateMachine = require('../memraft/statemachine');
var stream = require('stream');
var nodeunitPlus = require('nodeunit-plus');
var vasync = require('vasync');

// All the actual tests are here...
var commandLogTests = require('../share/command_log_tests.js');



///--- Globals

var after = nodeunitPlus.after;
var before = nodeunitPlus.before;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'leveldb_log-test',
    stream: process.stdout
});
var TMP_DIR = path.resolve(path.dirname(__dirname), '..') + '/tmp';
var DB_FILE = TMP_DIR + '/leveldb_log_test.db';



///--- Setup/teardown

before(function (cb) {
    assert.func(cb, 'cb');
    var self = this;
    vasync.pipeline({
        'funcs': [
            function mkTmpDir(_, subcb) {
                fs.mkdir(TMP_DIR, function (err) {
                    if (err && err.code !== 'EEXIST') {
                        return (subcb(err));
                    }
                    return (subcb());
                });
            },
            function initStateMachine(_, subcb) {
                self.stateMachine = new StateMachine({ 'log': LOG });
                self.stateMachine.on('ready', subcb);
            },
            function removeOldLevelDb(_, subcb) {
                helper.rmrf(DB_FILE, subcb);
            },
            function initDb(_, subcb) {
                self.clog = new LevelDbLog({
                    'log': LOG,
                    'location': DB_FILE,
                    'stateMachine': self.stateMachine
                });
                self.clog.on('ready', subcb);
                self.clog.on('error', subcb);
            }
        ]
    }, function (err) {
        cb(err);
    });
});


after(function (cb) {
    var self = this;
    vasync.pipeline({
        'funcs': [
            function closeLevelDb(_, subcb) {
                self.clog.close(subcb);
            }
        ]
    }, function (err) {
        cb(err);
    });
});
