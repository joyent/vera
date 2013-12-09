// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('../helper.js');
var LevelDbLog = require('../../lib/leveldb_log');
//TODO: Should be persisted.
var MemProps = require('../memraft/memprops');
var lib = require('../../lib');
//TODO: Move the message bus out somewhere?
var MessageBus = require('../memraft/messagebus');
var path = require('path');
var Raft = require('../../lib/raft');
//TODO: Should be persisted.
var StateMachine = require('../memraft/statemachine');
var vasync = require('vasync');

// All the actual tests are here...
var raftInitTests = require('../share/raft_init_tests.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});
var LOW_LEADER_TIMEOUT = 2;
var TMP_DIR = path.dirname(__dirname) + '/tmp';
var DB_FILE = TMP_DIR + '/leveldbraft_init_test.db';



///--- Setup/Teardown

//TODO: Need to factor this out.
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
            function initMessageBus(_, subcb) {
                self.messageBus = new MessageBus({ 'log': LOG });
                self.messageBus.on('ready', subcb);
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
            },
            function initMemProps(_, subcb) {
                self.properties = new MemProps({
                    'log': LOG,
                    'props': {
                        'currentTerm': 0
                    }
                });
                self.properties.on('ready', subcb);
            },
            function initRaft(_, subcb) {
                self.raft = new Raft({
                    'log': LOG,
                    'id': 'raft-0',
                    'peers': [ 'raft-1', 'raft-2' ],
                    'clog': self.clog,
                    'stateMachine': self.stateMachine,
                    'messageBus': self.messageBus,
                    'properties': self.properties
                });
                self.raft.leaderTimeout = LOW_LEADER_TIMEOUT;
                subcb();
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
