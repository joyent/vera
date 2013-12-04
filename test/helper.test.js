// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var helper = require('./helper.js');



///--- Globals

var e = helper.e;
var entryStream = helper.entryStream;
var memStream = helper.memStream;
var test = helper.test;



///--- Tests

test('e', function (t) {
    var e1 = e(0, 1);
    t.ok(e1);
    t.equal(0, e1.index);
    t.equal(1, e1.term);
    t.equal('noop', e1.command);

    var e2 = e(1, 2);
    t.equal(1, e2.index);
    t.equal(2, e2.term);
    t.equal('command-1-2', e2.command);
    t.done();
});


test('entryStream', function (t) {
    var s = entryStream([0, 0, 1, 2, 2, 4]);
    var entries = [];
    s.on('readable', function () {
        var d;
        while (null !== (d = s.read())) {
            entries.push(d);
        }
    });
    s.once('end', function () {
        t.deepEqual([
            { 'index': 0, 'term': 0, 'command': 'noop'},
            { 'index': 1, 'term': 2, 'command': 'command-1-2'},
            { 'index': 2, 'term': 4, 'command': 'command-2-4'}
        ], entries);
        t.done();
    });
});


test('test mem flowing', function (t) {
    var one = memStream([1, 2, 3]);
    var res = [];

    one.on('data', function (d) {
        res.push(d);
    });

    one.once('end', function () {
        t.deepEqual([1, 2, 3], res);
        t.done();
    });
});


test('test mem non-flowing', function (t) {
    var one = memStream([1, 2, 3]);
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


test('test mem end immediately flowing', function (t) {
    var one = memStream([]);
    var res = [];

    one.on('data', function (d) {
        res.push(d);
    });

    one.once('end', function () {
        t.deepEqual([], res);
        t.done();
    });
});


test('test mem end immediately non-flowing', function (t) {
    var one = memStream([]);
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


test('test mem stream after reading one, flowing', function (t) {
    var one = memStream([1, 2, 3, 4, 5]);
    var res = [];
    var firstOne = null;

    function startReadAll() {
        one.on('data', function (d) {
            res.push(d);
        });
    }

    function oneOnReadable() {
        firstOne = one.read();
        if (firstOne === null) {
            one.once('readable', oneOnReadable);
        } else {
            startReadAll();
        }
    }

    one.once('readable', oneOnReadable);
    one.once('end', function () {
        t.equal(1, firstOne);
        t.deepEqual([2, 3, 4, 5], res);
        t.done();
    });
});


test('test mem stream after reading one, non-flowing', function (t) {
    var one = memStream([1, 2, 3, 4, 5]);
    var res = [];
    var firstOne = null;

    function startReadAll() {
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
        if (firstOne === null) {
            one.once('readable', oneOnReadable);
        } else {
            startReadAll();
        }
    }

    one.once('readable', oneOnReadable);
    one.once('end', function () {
        t.equal(1, firstOne);
        t.deepEqual([2, 3, 4, 5], res);
        t.done();
    });
});
