// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('../helper.js');
var LevelDbLog = require('../../lib/leveldb/log');
var lib = require('../../lib/leveldb');
var memStream = require('../../lib').memStream;
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
var test = nodeunitPlus.test;
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
                    'stateMachine': self.stateMachine,
                    'clusterConfig': {}
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



///--- Level-db only tests


test('verify lastLogIndex in db', function (t) {
    var self = this;

    function getAndCompareIndex(expected, cb) {
        function onGet(err, v) {
            if (err) {
                return (cb(err));
            }
            t.equal(expected, v);
            cb();
        }
        self.clog.db.get(lib.internalPropertyKey('lastLogIndex'),
                         onGet);
    }

    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                //No index, so append!
                'entries': memStream([ { 'term': 2,
                                         'command': 'command-1-2' } ])
            }, function (err, entry) {
                if (err) {
                    subcb(err);
                    return;
                }
                t.equal(1, entry.index);
                t.deepEqual(self.clog.last(), entry);
                t.equal(2, self.clog.nextIndex);
                getAndCompareIndex(1, subcb);
            });
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                //No index, so append!
                'entries': memStream([ { 'term': 2,
                                         'command': 'command-2-2' } ])
            }, function (err, entry) {
                t.equal(2, entry.index);
                t.deepEqual(self.clog.last(), entry);
                getAndCompareIndex(2, subcb);
            });
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('open with correct state', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': memStream([
                    { 'term': 2, 'command': 'command-1-2' }
                ])
            }, function (err, entry) {
                if (err) {
                    subcb(err);
                    return;
                }
                t.equal(1, entry.index);
                t.deepEqual(self.clog.last(), entry);
                t.equal(2, self.clog.nextIndex);
                //Stash some stuff for later...
                _.last = self.clog.last();
                _.nextIndex = self.clog.nextIndex;
                subcb();
            });
        },
        //Close the level db
        function (_, subcb) {
            self.clog.close(subcb);
        },
        //Open a new clog over the old one
        function (_, subcb) {
            self.clog = new LevelDbLog({
                'log': LOG,
                'location': DB_FILE,
                'stateMachine': self.stateMachine
            });
            self.clog.on('ready', subcb);
            self.clog.on('error', subcb);
        },
        function (_, subcb) {
            t.deepEqual(_.last, self.clog.last());
            t.equal(_.nextIndex, self.clog.nextIndex);
            subcb();
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
