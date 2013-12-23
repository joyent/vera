// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var lib = require('../../lib');
var MemLog = require('./memlog');
var StateMachine = require('./statemachine');
var stream = require('stream');
var util = require('util');
var vasync = require('vasync');

// All the actual tests are here...
var commandLogTests = require('../share/command_log_tests.js');



///--- Globals

var before = helper.before;
var e = helper.e;
var entryStream = helper.entryStream;
var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memlog-test',
    stream: process.stdout
});



///--- Setup/Teardown

before(function (cb) {
    var self = this;
    vasync.pipeline({
        funcs: [
            function initStateMachine(_, subcb) {
                self.stateMachine = new StateMachine({ 'log': LOG });
                self.stateMachine.on('ready', subcb);
            },
            function initMemLogHere(_, subcb) {
                self.clog = new MemLog({ 'log': LOG,
                                    'stateMachine': self.stateMachine });
                self.clog.on('ready', subcb);
            }
        ]
    }, function (err) {
        cb(err);
    });
});



///--- Tests only for the memraft...

function NoopTransform(a) {
    stream.Transform.call(this, { 'objectMode': true });
}
util.inherits(NoopTransform, stream.Transform);

NoopTransform.prototype._transform = function (entry, encoding, cb) {
    this.push(entry);
    cb();
};


test('test noop transform entry stream', function (t) {
    var self = this;
    vasync.pipeline({
        funcs: [
            function (_, subcb) {
                self.clog.append({
                    'commitIndex': 0,
                    'term': 1,
                    'entries': entryStream([
                        0, 0,
                        1, 0,
                        2, 1,
                        3, 1
                    ])
                }, subcb);
            },
            function sliceTwo(_, subcb) {
                var res = [];
                self.clog.slice(0, 2, function (err, entries) {
                    var ns = new NoopTransform();
                    entries.pipe(ns);
                    entries = ns;
                    entries.on('readable', function () {
                        var d;
                        while (null !== (d = entries.read())) {
                            res.push(d);
                        }
                    });
                    entries.on('end', function () {
                        t.deepEqual([ e(0, 0), e(1, 0)], res);
                        subcb();
                    });
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
