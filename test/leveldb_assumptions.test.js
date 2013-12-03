// Copyright (c) 2013, Joyent, Inc. All rights reserved.

//This may be a liitle unconventional to keep around, but this file exists to
// show the ways vera uses leveldb and as a reminder of how to interact with it.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('./helper.js');
var levelup = require('level');
var path = require('path');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'leveldb_log-test',
    stream: process.stdout
});
var TMP_DIR = path.dirname(__dirname) + '/tmp';
var DB_FILE = TMP_DIR + '/leveldb_assumptions_test.db';



///--- Helpers

function e(index, term) {
    return ({
        'index': index,
        'term': term,
        'command': index === 0 ? 'noop' : 'command-' + index + '-' + term
    });
}


function le(x) {
    var b = new Buffer(4);
    b.writeUInt32BE(x, 0);
    return (b);
}


function dumpLog(db, cb) {
    db.createReadStream()
        .on('data', function (d) {
            //console.log(JSON.stringify(d, null, 0));
        }).on('close', cb);
}


function rmrf(f, cb) {
    fs.stat(f, function (err, stats) {
        if (err) {
            return (cb(err));
        }
        if (stats.isDirectory()) {
            fs.readdir(f, function (err2, files) {
                if (err2) {
                    return (cb(err2));
                }
                vasync.forEachPipeline({
                    'func': rmrf,
                    'inputs': files.map(function (x) { return (f + '/' + x); })
                }, function (err3) {
                    if (err3) {
                        return (cb(err3));
                    }
                    fs.rmdir(f, cb);
                });
            });
        } else {
            fs.unlink(f, cb);
        }
    });
}



///--- Tests

test('test assumptions', function (t) {
    var db;
    vasync.pipeline({
        'funcs': [
            function mkTmpDir(_, cb) {
                fs.mkdir(TMP_DIR, function (err) {
                    if (err && err.code !== 'EEXIST') {
                        return (cb(err));
                    }
                    return (cb());
                });
            },
            function initDb(_, cb) {
                var levelOpts = {
                    'keyEncoding': 'binary',
                    'valueEncoding': 'json'
                };
                levelup(DB_FILE, levelOpts, function (err, d) {
                    if (err) {
                        return (cb(err));
                    }
                    db = d;
                    cb();
                });
            },
            function writeStuff(_, cb) {
                db.batch([
                    { 'type': 'put', 'key': le(0), 'value': e(0, 0) },
                    { 'type': 'put', 'key': le(1), 'value': e(1, 0) },
                    { 'type': 'put', 'key': le(2), 'value': e(2, 1) },
                    { 'type': 'put', 'key': le(3), 'value': e(3, 1) },
                    { 'type': 'put', 'key': le(4), 'value': e(4, 2) }
                ], cb);
            },
            function iterMiddle(_, cb) {
                var rs = db.createReadStream({
                    'start': le(1),
                    'end': le(3)
                });
                var res = [];

                //Note: non-flowing!
                rs.on('readable', function () {
                    var d;
                    while (null !== (d = rs.read())) {
                        res.push(d.value.index);
                    }
                });

                rs.on('close', function () {
                    t.deepEqual([1, 2, 3], res);
                    cb();
                });
            },
            function iterMidToEnd(_, cb) {
                var rs = db.createReadStream({
                    'start': le(1),
                    'end': new Buffer('ffffffff', 'hex')
                });
                var res = [];

                //Note: non-flowing!
                rs.on('readable', function () {
                    var d;
                    while (null !== (d = rs.read())) {
                        res.push(d.value.index);
                    }
                });

                rs.on('close', function () {
                    cb();
                });
            },
            function iterAfterEnd(_, cb) {
                var rs = db.createReadStream({
                    'start': le(10),
                    'end': new Buffer('ffffffff', 'hex')
                });
                var res = [];

                rs.on('readable', function () {
                    var d;
                    while (null !== (d = rs.read())) {
                        res.push(d.value.index);
                    }
                });

                rs.on('close', function () {
                    t.deepEqual([], res);
                    cb();
                });
            },
            function oneThenFlowingThenDestroy(_, cb) {
                var rs = db.createReadStream({
                    'start': le(1),
                    'end': le(5)
                });
                var res = [];

                rs.once('readable', function () {
                    var d = rs.read();
                    t.equal(1, d.value.index);
                    var read = 0;
                    rs.on('data', function (data) {
                        res.push(data.value.index);
                        if (++read === 2) {
                            //Note the destroy function here!
                            rs.destroy();
                        }
                    });
                });

                rs.on('close', function () {
                    t.deepEqual([2, 3], res);
                    cb();
                });
            },
            function dump(_, cb) {
                dumpLog(db, cb);
            },
            function close(_, cb) {
                db.close(cb);
            },
            function deleteDb(_, cb) {
                rmrf(DB_FILE, cb);
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
