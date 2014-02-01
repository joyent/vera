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
        'command': index !== 0 ? 'command-' + index + '-' + term : 'noop'
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
            return (setImmediate(cb));
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
    'createLogger': createLogger,
    'e': e,
    'entryStream': entryStream,
    'readClog': readClog,
    'readStream': readStream,
    'rmrf': rmrf
};
