// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('./helper.js');
var lib = require('../lib');
var PairsStream = require('../lib/pairs_stream');
var Readable = require('stream').Readable;
var util = require('util');
var vasync = require('vasync');



///--- Globals

var memStream = lib.memStream;
var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memprops-test',
    stream: process.stdout
});
var PAUSE_MILLIS = 5;



///--- Helpers

function makeResult(pairs, left, right) {
    pairs = pairs || [];
    left = left || [];
    right = right || [];
    assert.equal(0, pairs.length % 2);

    var r = [];
    for (var i = 0; i < pairs.length; i += 2) {
        r.push({
            'left': pairs[i],
            'right': pairs[i + 1]
        });
    }
    for (i = 0; i < left.length; ++i) {
        r.push({
            'left': left[i]
        });
    }
    for (i = 0; i < right.length; ++i) {
        r.push({
            'right': right[i]
        });
    }
    return (r);
}


function runFlowingTest(opts, cb) {
    assert.object(opts.t);
    assert.bool(opts.flowing);
    assert.object(opts.result);

    var t = opts.t;
    var flowing = opts.flowing;
    var left = memStream(opts.left || []);
    var right = memStream(opts.right || []);
    var result = opts.result;
    var res = [];

    var ps = new PairsStream({ 'left': left, 'right': right });

    if (flowing) {
        ps.on('data', function (d) {
            res.push(d);
        });
    } else {
        ps.on('readable', function () {
            var d;
            while (null !== (d = ps.read())) {
                res.push(d);
            }
        });
    }

    ps.on('end', function () {
        t.ok(ps.leftEnded);
        t.ok(ps.rightEnded);
        t.deepEqual(result, res);
        cb();
    });
}


function runTest(opts) {
    opts.flowing = false;
    runFlowingTest(opts, function () {
        opts.flowing = true;
        runFlowingTest(opts, function () {
            opts.t.done();
        });
    });
}



///--- Tests for the PairsStream class

test('no elements in streams', function (t) {
    runTest({
        't': t,
        'left': [], 'right': [],
        'result': makeResult()
    });
});


test('one element in streams', function (t) {
    runTest({
        't': t,
        'left': [1], 'right': [2],
        'result': makeResult([1, 2])
    });
});


test('end at same time', function (t) {
    runTest({
        't': t,
        'left': [1, 2, 3], 'right': [4, 5, 6],
        'result': makeResult([ 1, 4, 2, 5, 3, 6 ])
    });
});


test('one element in left', function (t) {
    runTest({
        't': t,
        'left': [1], 'right': [],
        'result': makeResult(null, [1])
    });
});


//I know this is looking super-pedantic, but this actually was the only failing
// test with an earlier version of the PairsStream class.
test('one element in left, 2 in right', function (t) {
    runTest({
        't': t,
        'left': [1], 'right': [2, 3],
        'result': makeResult([ 1, 2 ], null, [3])
    });
});


test('one element in right', function (t) {
    runTest({
        't': t,
        'left': [], 'right': [1],
        'result': makeResult(null, null, [1])
    });
});


test('one element in right, 2 in left', function (t) {
    runTest({
        't': t,
        'left': [1, 2], 'right': [3],
        'result': makeResult([ 1, 3 ], [2])
    });
});


test('right ends 1 before left', function (t) {
    runTest({
        't': t,
        'left': [1, 2, 3, 10], 'right': [4, 5, 6],
        'result': makeResult([ 1, 4, 2, 5, 3, 6 ], [10])
    });
});


test('left ends 1 before right', function (t) {
    runTest({
        't': t,
        'left': [1, 2, 3], 'right': [4, 5, 6, 10],
        'result': makeResult([ 1, 4, 2, 5, 3, 6 ], null, [10])
    });
});


test('right ends 2 before left', function (t) {
    runTest({
        't': t,
        'left': [1, 2, 3, 10, 11], 'right': [4, 5, 6],
        'result': makeResult([ 1, 4, 2, 5, 3, 6 ], [10, 11])
    });
});


test('left ends 2 before right', function (t) {
    runTest({
        't': t,
        'left': [1, 2, 3], 'right': [4, 5, 6, 10, 11],
        'result': makeResult([ 1, 4, 2, 5, 3, 6 ], null, [10, 11])
    });
});


test('read slow, non-flowing, left ends first', function (t) {
    var left = memStream([1, 2, 3, 4]);
    var right = memStream([1, 2, 3, 4, 5, 6, 7, 8]);
    var res = [];

    var ps = new PairsStream({ 'left': left, 'right': right });

    ps.on('readable', function () {
        function next() {
            var d = ps.read();
            if (d !== null) {
                res.push(d);
                setTimeout(next, PAUSE_MILLIS);
            }
        }
        setTimeout(next, PAUSE_MILLIS);
    });

    ps.on('end', function () {
        t.ok(ps.leftEnded);
        t.ok(ps.rightEnded);
        t.deepEqual(makeResult([1, 1, 2, 2, 3, 3, 4, 4], null,
                               [5, 6, 7, 8]), res);
        t.done();
    });
});


test('read slow, non-flowing, right ends first', function (t) {
    var left = memStream([1, 2, 3, 4, 5, 6, 7, 8]);
    var right = memStream([1, 2, 3, 4]);
    var res = [];

    var ps = new PairsStream({ 'left': left, 'right': right });

    ps.on('readable', function () {
        function next() {
            var d = ps.read();
            if (d !== null) {
                res.push(d);
                setTimeout(next, PAUSE_MILLIS);
            }
        }
        setTimeout(next, PAUSE_MILLIS);
    });

    ps.on('end', function () {
        t.ok(ps.leftEnded);
        t.ok(ps.rightEnded);
        t.deepEqual(makeResult([1, 1, 2, 2, 3, 3, 4, 4],
                               [5, 6, 7, 8]), res);
        t.done();
    });
});


test('pause during pairs, flowing', function (t) {
    var left = memStream([1, 2, 3, 4, 5, 6, 7, 8]);
    var right = memStream([1, 2, 3, 4]);
    var res = [];
    var paused = false;
    var npaused = 0;

    var ps = new PairsStream({ 'left': left, 'right': right });

    ps.on('data', function (d) {
        assert.ok(!paused);
        res.push(d);
        paused = true;
        ++npaused;
        ps.pause();
        setTimeout(function () {
            paused = false;
            ps.resume();
        }, PAUSE_MILLIS);
    });

    ps.on('end', function () {
        t.ok(ps.leftEnded);
        t.ok(ps.rightEnded);
        t.deepEqual(makeResult([1, 1, 2, 2, 3, 3, 4, 4],
                               [5, 6, 7, 8]), res);
        t.equal(8, npaused);
        t.done();
    });
});


test('pause during left still going, flowing', function (t) {
    var left = memStream([1, 2, 3, 4, 5, 6, 7, 8]);
    var right = memStream([1, 2, 3, 4]);
    var res = [];
    var paused = false;
    var npaused = 0;

    var ps = new PairsStream({ 'left': left, 'right': right });

    ps.on('data', function (d) {
        assert.ok(!paused);
        res.push(d);
        if (d.right === undefined) {
            paused = true;
            ++npaused;
            ps.pause();
            setTimeout(function () {
                paused = false;
                ps.resume();
            }, PAUSE_MILLIS);
        }
    });

    ps.on('end', function () {
        t.ok(ps.leftEnded);
        t.ok(ps.rightEnded);
        t.deepEqual(makeResult([1, 1, 2, 2, 3, 3, 4, 4],
                               [5, 6, 7, 8]), res);
        t.equal(4, npaused);
        t.done();
    });
});


test('pause during right still going, flowing', function (t) {
    var left = memStream([1, 2, 3, 4]);
    var right = memStream([1, 2, 3, 4, 5, 6, 7, 8]);
    var res = [];
    var paused = false;
    var npaused = 0;

    var ps = new PairsStream({ 'left': left, 'right': right });

    ps.on('data', function (d) {
        assert.ok(!paused);
        res.push(d);
        if (d.left === undefined) {
            paused = true;
            ps.pause();
            ++npaused;
            setTimeout(function () {
                paused = false;
                ps.resume();
            }, PAUSE_MILLIS);
        }
    });

    ps.on('end', function () {
        t.ok(ps.leftEnded);
        t.ok(ps.rightEnded);
        t.deepEqual(makeResult([1, 1, 2, 2, 3, 3, 4, 4], null,
                               [5, 6, 7, 8]), res);
        t.equal(4, npaused);
        t.done();
    });
});


test('stream after reading one', function (t) {
    var left = memStream([1, 2, 3, 4]);
    var right = memStream([1, 2, 3, 4, 5, 6, 7, 8]);
    var res = [];

    var leftFirst = null;
    var rightFirst = null;

    function leftOnReadable() {
        leftFirst = left.read();
        if (leftFirst === null) {
            left.once('readable', leftOnReadable);
        } else {
            tryStartReadable();
        }
    }

    function rightOnReadable() {
        rightFirst = right.read();
        if (rightFirst === null) {
            right.once('readable', rightOnReadable);
        } else {
            tryStartReadable();
        }
    }

    function tryStartReadable() {
        if (leftFirst === null || rightFirst === null) {
            return;
        }

        var ps = new PairsStream({ 'left': left, 'right': right });

        ps.on('data', function (d) {
            res.push(d);
        });

        ps.on('end', function () {
            t.ok(ps.leftEnded);
            t.ok(ps.rightEnded);
            t.equal(1, leftFirst);
            t.equal(1, rightFirst);
            t.deepEqual(makeResult([2, 2, 3, 3, 4, 4], null,
                                   [5, 6, 7, 8]), res);
            t.done();
        });
    }

    left.once('readable', leftOnReadable);
    right.once('readable', rightOnReadable);
});


test('stream after reading one, left end', function (t) {
    var left = memStream([1]);
    var right = memStream([1, 2]);
    var res = [];

    var leftFirst = null;
    var rightFirst = null;

    function leftOnReadable() {
        leftFirst = left.read();
        if (leftFirst === null) {
            left.once('readable', leftOnReadable);
        } else {
            tryStartReadable();
        }
    }

    function rightOnReadable() {
        rightFirst = right.read();
        if (rightFirst === null) {
            right.once('readable', rightOnReadable);
        } else {
            tryStartReadable();
        }
    }

    function tryStartReadable() {
        if (leftFirst === null || rightFirst === null) {
            return;
        }

        var ps = new PairsStream({ 'left': left, 'right': right });

        ps.on('data', function (d) {
            res.push(d);
        });

        ps.on('end', function () {
            t.ok(ps.leftEnded);
            t.ok(ps.rightEnded);
            t.equal(1, leftFirst);
            t.equal(1, rightFirst);
            t.deepEqual(makeResult(null, null, [2]), res);
            t.done();
        });
    }

    left.once('readable', leftOnReadable);
    right.once('readable', rightOnReadable);
});


test('stream after reading one, right end', function (t) {
    var left = memStream([1, 2]);
    var right = memStream([1]);
    var res = [];

    var leftFirst = null;
    var rightFirst = null;

    function leftOnReadable() {
        leftFirst = left.read();
        if (leftFirst === null) {
            left.once('readable', leftOnReadable);
        } else {
            tryStartReadable();
        }
    }

    function rightOnReadable() {
        rightFirst = right.read();
        if (rightFirst === null) {
            right.once('readable', rightOnReadable);
        } else {
            tryStartReadable();
        }
    }

    function tryStartReadable() {
        if (leftFirst === null || rightFirst === null) {
            return;
        }

        var ps = new PairsStream({ 'left': left, 'right': right });

        ps.on('data', function (d) {
            res.push(d);
        });

        ps.on('end', function () {
            t.ok(ps.leftEnded);
            t.ok(ps.rightEnded);
            t.equal(1, leftFirst);
            t.equal(1, rightFirst);
            t.deepEqual(makeResult(null, [2]), res);
            t.done();
        });
    }

    left.once('readable', leftOnReadable);
    right.once('readable', rightOnReadable);
});


test('left ends while waiting for right readable', function (t) {
    /**
     * Need a very specific set of events:
     * 1) left is read
     * 2) right reads null (not ready or ended yet)
     * 3) left ends
     * 4) right ends
     */
    var right = Readable({ 'objectMode': true });
    var rightPushed = false;
    right._read = function () {
        var self = this;
        if (!rightPushed) {
            self.push(1);
            rightPushed = true;
        }
        // 2) right reads null
    };

    var left = Readable({ 'objectMode': true });
    var leftPushed = false;
    left._read = function () {
        var self = this;
        if (leftPushed) {
            return;
        }
        self.push(1);
        // 1) left is read
        self.push(2);
        function onWaitingForReadable() {
            t.ok(self.ps.waitingForReadable);
            t.ok(!self.ps.leftEnded);
            t.ok(!self.ps.rightEnded);
            self.once('end', function () {
                t.ok(self.ps.waitingForReadable);
                t.ok(self.ps.leftEnded);
                t.ok(!self.ps.rightEnded);
                //4) right ends
                right.push(null);
            });
            // 3) left ends
            self.push(null);
        }
        process.nextTick(function () {
            //ps should have been set by the next tick.
            t.ok(self.ps);
            if (!self.ps.waitingForReadable) {
                self.ps.on('waitingForReadable', onWaitingForReadable);
            } else {
                onWaitingForReadable();
            }
        });
        leftPushed = true;
    };

    var ps = new PairsStream({ 'left': left, 'right': right });
    left.ps = ps;
    var res = [];

    ps.on('data', function (d) {
        res.push(d);
    });

    ps.on('end', function () {
        t.ok(ps.leftEnded);
        t.ok(ps.rightEnded);
        t.deepEqual(makeResult([ 1, 1 ], [ 2 ]), res);
        t.done();
    });

});
