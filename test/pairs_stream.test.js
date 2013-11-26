// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('./helper.js');
var PairsStream = require('../lib/pairs_stream');
var Readable = require('stream').Readable;
var util = require('util');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memprops-test',
    stream: process.stdout
});
var PAUSE_MILLIS = 5;


///--- Helpers

function ns(a) {
    var r = Readable({ objectMode: true });
    r.ended = false;
    r.i = 0;
    r._read = function () {
        if (r.ended) {
            return;
        }
        if (r.i !== a.length) {
            r.push(a[r.i++]);
        }
        if (r.i === a.length) {
            r.ended = true;
            r.push(null);
        }
    };
    return (r);
}


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
    var left = ns(opts.left || []);
    var right = ns(opts.right || []);
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



///--- Tests for our internal ns class

test('test ns flowing', function (t) {
    var one = ns([1, 2, 3]);
    var res = [];

    one.on('data', function (d) {
        res.push(d);
    });

    one.once('end', function () {
        t.deepEqual([1, 2, 3], res);
        t.done();
    });
});


test('test ns non-flowing', function (t) {
    var one = ns([1, 2, 3]);
    var res = [];

    one.on('readable', function () {
        var d;
        while (null !== (d = one.read())) {
            res.push(d);
        }
    });

    one.once('end', function () {
        t.deepEqual([1, 2, 3], res);
        t.done();
    });
});


test('test ns end immediately flowing', function (t) {
    var one = ns([]);
    var res = [];

    one.on('data', function (d) {
        res.push(d);
    });

    one.once('end', function () {
        t.deepEqual([], res);
        t.done();
    });
});


test('test ns end immediately non-flowing', function (t) {
    var one = ns([]);
    var res = [];

    one.on('readable', function () {
        var d;
        while (null !== (d = one.read())) {
            res.push(d);
        }
    });

    one.once('end', function () {
        t.deepEqual([], res);
        t.done();
    });
});


test('test ns stream after reading one, flowing', function (t) {
    var one = ns([1, 2, 3, 4, 5]);
    var res = [];
    var firstOne = null;

    function tryStartReadAll() {
        if (firstOne === null) {
            return;
        }
        one.removeListener('readable', oneOnReadable);

        //Start it flowing...
        one.on('data', function (d) {
            res.push(d);
        });
    }

    function oneOnReadable() {
        firstOne = one.read();
        tryStartReadAll();
    }

    one.on('readable', oneOnReadable);
    one.once('end', function () {
        t.equal(1, firstOne);
        t.deepEqual([2, 3, 4, 5], res);
        t.done();
    });
});


test('test ns stream after reading one, non-flowing', function (t) {
    var one = ns([1, 2, 3, 4, 5]);
    var res = [];
    var firstOne = null;

    function tryStartReadAll() {
        if (firstOne === null) {
            return;
        }
        one.removeListener('readable', oneOnReadable);

        function restReadable() {
            var d;
            while (null !== (d = one.read())) {
                res.push(d);
            }
        }
        one.on('readable', restReadable);
    }

    function oneOnReadable() {
        firstOne = one.read();
        tryStartReadAll();
    }

    one.on('readable', oneOnReadable);
    one.once('end', function () {
        t.equal(1, firstOne);
        t.deepEqual([2, 3, 4, 5], res);
        t.done();
    });
});



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


//test('right ends 1 before left', function (t) {
//test('left ends 1 before right', function (t) {
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
    var left = ns([1, 2, 3, 4]);
    var right = ns([1, 2, 3, 4, 5, 6, 7, 8]);
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
    var left = ns([1, 2, 3, 4, 5, 6, 7, 8]);
    var right = ns([1, 2, 3, 4]);
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
    var left = ns([1, 2, 3, 4, 5, 6, 7, 8]);
    var right = ns([1, 2, 3, 4]);
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
    var left = ns([1, 2, 3, 4, 5, 6, 7, 8]);
    var right = ns([1, 2, 3, 4]);
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
    var left = ns([1, 2, 3, 4]);
    var right = ns([1, 2, 3, 4, 5, 6, 7, 8]);
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
    var left = ns([1, 2, 3, 4]);
    var right = ns([1, 2, 3, 4, 5, 6, 7, 8]);
    var res = [];

    var leftFirst = null;
    var rightFirst = null;

    function leftOnReadable() {
        leftFirst = left.read();
        tryStartReadable();
    }

    function rightOnReadable() {
        rightFirst = right.read();
        tryStartReadable();
    }

    function tryStartReadable() {
        if (leftFirst === null || rightFirst === null) {
            return;
        }
        left.removeListener('readable', leftOnReadable);
        right.removeListener('readable', rightOnReadable);

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

    left.on('readable', leftOnReadable);
    right.on('readable', rightOnReadable);
});


test('stream after reading one, left end', function (t) {
    var left = ns([1]);
    var right = ns([1, 2]);
    var res = [];

    var leftFirst = null;
    var rightFirst = null;

    function leftOnReadable() {
        leftFirst = left.read();
        tryStartReadable();
    }

    function rightOnReadable() {
        rightFirst = right.read();
        tryStartReadable();
    }

    function tryStartReadable() {
        if (leftFirst === null || rightFirst === null) {
            return;
        }
        left.removeListener('readable', leftOnReadable);
        right.removeListener('readable', rightOnReadable);

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

    left.on('readable', leftOnReadable);
    right.on('readable', rightOnReadable);
});


test('stream after reading one, right end', function (t) {
    var left = ns([1, 2]);
    var right = ns([1]);
    var res = [];

    var leftFirst = null;
    var rightFirst = null;

    function leftOnReadable() {
        leftFirst = left.read();
        tryStartReadable();
    }

    function rightOnReadable() {
        rightFirst = right.read();
        tryStartReadable();
    }

    function tryStartReadable() {
        if (leftFirst === null || rightFirst === null) {
            return;
        }
        left.removeListener('readable', leftOnReadable);
        right.removeListener('readable', rightOnReadable);

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

    left.on('readable', leftOnReadable);
    right.on('readable', rightOnReadable);
});
