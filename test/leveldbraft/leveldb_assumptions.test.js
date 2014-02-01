// Copyright (c) 2013, Joyent, Inc. All rights reserved.

//This may be a liitle unconventional to keep around, but this file exists to
// show the ways vera uses leveldb and as a reminder of how to interact with it.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('../helper.js');
var http = require('http');
var lstream = require('lstream');
var levelup = require('level');
var path = require('path');
var test = require('nodeunit-plus').test;
var Transform = require('stream').Transform;
var vasync = require('vasync');



///--- Globals

var e = helper.e;
var rmrf = helper.rmrf;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'leveldb_log-test',
    stream: process.stdout
});
var TMP_DIR = path.resolve(path.dirname(__dirname), '..') + '/tmp';
var DB_FILE = TMP_DIR + '/leveldb_assumptions_test.db';



///--- Helpers

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
            function iterStartEndEqual(_, cb) {
                var rs = db.createReadStream({
                    'start': le(2),
                    'end': le(2)
                });
                var res = [];

                rs.on('readable', function () {
                    var d;
                    while (null !== (d = rs.read())) {
                        res.push(d.value.index);
                    }
                });

                rs.on('close', function () {
                    t.deepEqual([ 2 ], res);
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


test('types in a json object', function (t) {
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
                    { 'type': 'put', 'key': le(0), 'value': {
                        'int': 5,
                        'arr': [0, 1, 2],
                        'obj': { 'foo': 'bar' },
                        'str': 'bar',
                        'buf': new Buffer('baz', 'utf-8')
                    }}
                ], cb);
            },
            function getBackCheckTypes(_, cb) {
                db.get(le(0), function (err, v) {
                    t.equal('[object Number]',
                            Object.prototype.toString.call(v.int));
                    t.equal('[object Array]',
                            Object.prototype.toString.call(v.arr));
                    t.equal('[object Object]',
                            Object.prototype.toString.call(v.obj));
                    t.equal('[object String]',
                            Object.prototype.toString.call(v.str));
                    //Note that we don't have a buffer anymore...
                    // makes sense though cause it's probably just doing a
                    // JSON.stringify.
                    t.equal('[object Array]',
                            Object.prototype.toString.call(v.buf));
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


test('piping leveldb across network', function (t) {
    var db;
    var otherdb;
    var server;
    var otherDbFile = DB_FILE + '-2';
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
            function initOtherDb(_, cb) {
                var levelOpts = {
                    'keyEncoding': 'binary',
                    'valueEncoding': 'json'
                };
                levelup(otherDbFile, levelOpts, function (err, d) {
                    if (err) {
                        return (cb(err));
                    }
                    otherdb = d;
                    cb();
                });
            },
            function startServer(_, cb) {
                server = http.createServer(function (req, res) {
                    res.writeHead(200, {
                        'Transfer-Encoding': 'chunked'
                    });
                    var rs = db.createReadStream();
                    rs.on('data', function (data) {
                        //This side stringifies the data and makes it
                        // newline, json separated.
                        res.write(JSON.stringify(data, null, 0) + '\n');
                    });
                    rs.on('end', function () {
                        res.end();
                    });
                    rs.on('error', function (err) {
                        console.error(err);
                        res.end();
                    });
                });
                server.listen(28080);
                cb();
            },
            function suckToOtherDb(_, cb) {
                http.get('http://localhost:28080', function (res) {
                    var trans = Transform({ objectMode: true });
                    trans._transform = function (data, encoding, subcb) {
                        if (data === '') {
                            return (subcb());
                        }
                        //This side decodes the json and trns it back into a
                        // stream of objects for the db to put.
                        var d = JSON.parse(data);
                        d.key = new Buffer(d.key);
                        this.push(d);
                        subcb();
                    };
                    res.pipe(new lstream()).pipe(trans).pipe(
                        otherdb.createWriteStream());
                    res.on('end', cb);
                }).on('error', function (err) {
                    console.error(err);
                    cb(err);
                });
            },
            function closeServer(_, cb) {
                server.close(cb);
            },
            function printOtherDb(_, cb) {
                var res = [];
                otherdb.createReadStream()
                    .on('data', function (data) {
                        res.push(data.value);
                    }).on('end', function () {
                        var expected = [
                            e(0, 0),
                            e(1, 0),
                            e(2, 1),
                            e(3, 1),
                            e(4, 2)
                        ];
                        t.deepEqual(expected, res);
                        t.done();
                        cb();
                    });
            },
            function close(_, cb) {
                db.close(cb);
            },
            function deleteDb(_, cb) {
                rmrf(DB_FILE, cb);
            },
            function closeOther(_, cb) {
                otherdb.close(cb);
            },
            function deleteOtherDb(_, cb) {
                rmrf(otherDbFile, cb);
            }
        ]
    }, function (err) {
        if (err) {
            console.log(err.name);
            console.log(err.code);
            console.log(err.message);
            t.fail(err);
        }
    });
});
