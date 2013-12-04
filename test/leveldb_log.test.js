// Copyright (c) 2013, Joyent, Inc. All rights reserved.

//TODO: This and the memlog test really should be the same thing.  This is just
// an initial playground to be removed after refactoring for streams.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('./helper.js');
var LevelDbLog = require('../lib/leveldb_log');
var lib = require('../lib');
var path = require('path');
var StateMachine = require('./memraft/statemachine');
var stream = require('stream');
var vasync = require('vasync');



///--- Globals

var after = helper.after;
var before = helper.before;
var test = helper.test;
var e = helper.e;
var entryStream = helper.entryStream;
var memStream = lib.memStream;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'leveldb_log-test',
    stream: process.stdout
});
var TMP_DIR = path.dirname(__dirname) + '/tmp';
var DB_FILE = TMP_DIR + '/leveldb_log_test.db';



///--- Helpers

function initLevelDbLog(opts, cb) {
    assert.func(cb, 'cb');
    var leveldbLog = null;
    vasync.pipeline({
        'arg': {},
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
                _.stateMachine = new StateMachine({ 'log': LOG });
                _.stateMachine.on('ready', subcb);
            },
            function initDb(_, subcb) {
                leveldbLog = new LevelDbLog({
                    'log': LOG,
                    'location': DB_FILE,
                    'stateMachine': _.stateMachine
                });
                leveldbLog.on('ready', subcb);
                leveldbLog.on('error', subcb);
            },
            function writeStuff(_, subcb) {
                //Kinda have to assume that the happy-case append works
                leveldbLog.append({
                    'commitIndex': 0,
                    'term': 2,
                    'entries': entryStream([0, 0, 1, 0, 2, 1, 3, 1, 4, 2])
                }, subcb);
            }
        ]
    }, function (err) {
        if (err) {
            console.error(err);
            process.exit(1);
        }
        cb(err, leveldbLog);
    });
}


///--- Setup/teardown

var LLOG;
before(function (cb) {
    initLevelDbLog({}, function (err, levelDbLog) {
        if (err) {
            return (cb(err));
        }
        LLOG = levelDbLog;
        cb();
    });
});


after(function (cb) {
    vasync.pipeline({
        'funcs': [
            function (_, subcb) {
                LLOG.close(subcb);
            }
        ]
    }, function (err) {
        cb(err);
    });
});



///--- Tests

test('test pass consistency check', function (t) {
    vasync.pipeline({
        'arg': {},
        'funcs': [
            function append(_, cb) {
                LLOG.append({
                    'commitIndex': 0,
                    'term': 2,
                    'entries': entryStream([0, 0, 1, 0, 2, 1])
                }, cb);
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('test append one', function (t) {
    vasync.pipeline({
        'arg': {},
        'funcs': [
            function append(_, cb) {
                var entry = { 'term': 2, 'command': 'frist psot!'};
                LLOG.append({
                    'commitIndex': 0,
                    'term': 2,
                    'entries': memStream([ entry ])
                }, function (err, ent) {
                    if (err) {
                        console.log(err.name);
                        return (cb(err));
                    }
                    assert.ok(ent);
                    assert.equal(5, ent.index);
                    cb();
                });
            }
        ]
    }, function (err) {
        if (err) {
            console.log(err.name);
            t.fail(err);
        }
        t.done();
    });
});
