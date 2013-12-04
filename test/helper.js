// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var lib = require('../lib');


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
    entryStream: entryStream
};
