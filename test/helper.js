// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var lib = require('../lib');
var vasync = require('vasync');



///--- Helpers

function createLogger(name, outputStream) {
    var log = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: name || process.argv[1],
        stream: outputStream || process.stdout,
        src: true,
        serializers: bunyan.stdSerializers
    });
    return (log);
}


function e(index, term) {
    return ({
        'index': index,
        'term': term,
        'command': index === 0 ? 'noop' : 'command-' + index + '-' + term
    });
}


function entryStream(a) {
    assert.equal(0, a.length % 2);
    var entries = [];
    for (var i = 0; i < a.length; i += 2) {
        entries.push(e(a[i], a[i + 1]));
    }
    return (lib.memStream(entries));
}


function readStream(s, cb) {
    var res = [];
    s.on('readable', function () {
        var d;
        while (null !== (d = s.read())) {
            res.push(d);
        }
    });
    s.on('error', function (err) {
        s.removeAllListeners();
        cb(err);
    });
    s.on('end', function () {
        cb(null, res);
    });
}


function readClog(clog, cb) {
    clog.slice(0, function (err, es) {
        if (err) {
            return (cb(err));
        }
        readStream(es, cb);
    });
}


function rmrf(f, cb) {
    fs.stat(f, function (err, stats) {
        if (err && err.code === 'ENOENT') {
            return (process.nextTick(cb));
        }
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



///--- Exports

module.exports = {

    after: function after(teardown) {
        module.parent.exports.tearDown = function _teardown(callback) {
            try {
                teardown.call(this, callback);
            } catch (err) {
                console.error('after:\n' + err.stack);
                process.exit(1);
            }
        };
    },

    before: function before(setup) {
        module.parent.exports.setUp = function _setup(callback) {
            try {
                setup.call(this, callback);
            } catch (err) {
                console.error('before:\n' + err.stack);
                process.exit(1);
            }
        };
    },

    test: function test(name, tester) {
        module.parent.exports[name] = function _(t) {
            var _done = false;
            t.end = function end() {
                if (!_done) {
                    _done = true;
                    t.done();
                }
            };
            t.notOk = function notOk(ok, message) {
                return (t.ok(!ok, message));
            };

            tester.call(this, t);
        };
    },

    createLogger: createLogger,
    e: e,
    entryStream: entryStream,
    readClog: readClog,
    readStream: readStream,
    rmrf: rmrf
};
