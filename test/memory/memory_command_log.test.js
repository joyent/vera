// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var lib = require('../../lib');
var MemLog = require('../../lib/memory/command_log');
var nodeunitPlus = require('nodeunit-plus');
var StateMachine = require('../../lib/memory/state_machine');
var stream = require('stream');
var util = require('util');
var vasync = require('vasync');

// Almost all the actual tests are here...
var commandLogTests = require('../share/command_log_tests.js');



///--- Globals

var before = nodeunitPlus.before;
var e = helper.e();
var entryStream = helper.entryStream();
var test = nodeunitPlus.test;
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
                                         'stateMachine': self.stateMachine,
                                         'clusterConfig': { 'current': {} }
                                       });
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


test('clone', function (t) {
    var self = this;
    var sm = self.stateMachine;
    var cl = self.clog;
    vasync.pipeline({
        funcs: [
            function (_, subcb) {
                t.deepEqual({
                    'clog': [ e(0, 0) ],
                    'clusterConfigIndex': 0
                }, cl.snapshot());
                subcb();
            },
            function (_, subcb) {
                cl.append({
                    'commitIndex': 0,
                    'term': 1,
                    'entries': entryStream([
                        0, 0,
                        1, 0,
                        2, 1
                    ])
                }, subcb);
            },
            function (_, subcb) {
                t.deepEqual({
                    'clog': [ e(0, 0),
                              e(1, 0),
                              e(2, 1) ],
                    'clusterConfigIndex': 0
                }, cl.snapshot());
                subcb();
            },
            function (_, subcb) {
                //It's OK to use the same state machine here for the test
                // because we know we won't care about it ever after.  This is
                // not safe to do elsewhere.
                cl.from(cl.snapshot(), sm, function (err, clClone) {
                    t.deepEqual({
                        'clog': [ e(0, 0),
                                  e(1, 0),
                                  e(2, 1) ],
                        'clusterConfigIndex': 0
                    }, clClone.snapshot());
                    t.equal(3, clClone.nextIndex);
                    clClone.append({
                        'commitIndex': 0,
                        'term': 1,
                        'entries': entryStream([
                            2, 1,
                            3, 1
                        ])
                    }, function () {
                        t.deepEqual({
                            'clog': [ e(0, 0),
                                      e(1, 0),
                                      e(2, 1),
                                      e(3, 1) ],
                            'clusterConfigIndex': 0
                        }, clClone.snapshot());
                        t.equal(4, clClone.nextIndex);
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
