/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('../helper.js');
var lib = require('../../lib/leveldb');
var path = require('path');
var stream = require('stream');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'leveldb_dbHelpers-test',
    stream: process.stdout
});
var TMP_DIR = path.resolve(path.dirname(__dirname), '..') + '/tmp';
var DB_FILE = TMP_DIR + '/leveldb_dbHelpers_test.db';
var KEY = new Buffer('00', 'hex');
var VALUE = { 'foo': 'bar' };


///--- Tests

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
                lib.dbHelpers.createOrOpen({
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
                lib.dbHelpers.createOrOpen({
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
