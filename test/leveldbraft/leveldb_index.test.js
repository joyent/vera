// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var buffertools = require('buffertools');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('../helper.js');
var leveldbIndex = require('../../lib/leveldb');
var path = require('path');
var StateMachine = require('../memraft/statemachine');
var stream = require('stream');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'leveldb_index-test',
    stream: process.stdout
});
var TMP_DIR = path.resolve(path.dirname(__dirname), '..') + '/tmp';
var DB_FILE = TMP_DIR + '/leveldb_index_test.db';
var KEY = new Buffer('00', 'hex');
var VALUE = { 'foo': 'bar' };


///--- Helpers

function runLogKeyTest(t, i) {
    var enc = leveldbIndex.logKey(i);
    var dec = leveldbIndex.logKeyDecode(enc);
    t.equal(i, dec);
}



///--- Tests

test('property key', function (t) {
    var s = 'foobarbaz';
    var enc = leveldbIndex.propertyKey(s);
    var dec = leveldbIndex.propertyKeyDecode(enc);
    t.equal(s, dec);
    t.done();
});


test('internal property key', function (t) {
    var s = 'foobarbaz';
    var enc = leveldbIndex.internalPropertyKey(s);
    var dec = leveldbIndex.propertyKeyDecode(enc);
    t.equal(s, dec);
    t.done();
});


test('log key', function (t) {
    runLogKeyTest(t, 0);
    runLogKeyTest(t, 1);
    runLogKeyTest(t, 4294967295);
    runLogKeyTest(t, 4294967296);
    //Javascript's max integer - 1.
    runLogKeyTest(t, 9007199254740991);
    //Javascript's max integer.
    assert.throws(function () {
        runLogKeyTest(t, 9007199254740992);
    });
    //Javascript's max integer + 1 (an invalid integer).
    assert.throws(function () {
        runLogKeyTest(t, 9007199254740993);
    });
    t.done();
});


test('log key ordering', function (t) {
    var a = [9007199254740991, 2, 1, 4294967295, 0,
             9007199254740991, 4294967296];
    var a_sorted = [0, 1, 2, 4294967295, 4294967296,
                    9007199254740991, 9007199254740991];
    var c = a.map(function (n) { return (leveldbIndex.logKey(n)); });
    //Compare comes from buffertools
    c.sort(function (l, r) { return (l.compare(r)); });
    t.equal(a.length, c.length);
    for (var i = 0; i < a.length; ++i) {
        t.equal(a_sorted[i], leveldbIndex.logKeyDecode(c[i]));
    }
    t.done();
});


test('create new log, close and open', function (t) {
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
            function removeOldLevelDb(_, subcb) {
                helper.rmrf(DB_FILE, subcb);
            },
            function createDb(_, subcb) {
                leveldbIndex.createOrOpen({
                    'log': LOG,
                    'location': DB_FILE
                }, function (err, res) {
                    if (err) {
                        subcb(err);
                    }
                    t.ok(res.created);
                    _.db = res.db;
                    subcb();
                });
            },
            function putFirst(_, subcb) {
                _.db.put(KEY, VALUE, subcb);
            },
            function close(_, subcb) {
                _.db.close(subcb);
            },
            function openAgain(_, subcb) {
                leveldbIndex.createOrOpen({
                    'log': LOG,
                    'location': DB_FILE
                }, function (err, res) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.ok(res.created === false);
                    _.reopened = res.db;
                    subcb();
                });
            },
            function getAndCheckKey(_, subcb) {
                _.reopened.get(KEY, function (err, value) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.deepEqual(VALUE, value);
                    subcb();
                });
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
