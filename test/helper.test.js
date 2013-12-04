// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var helper = require('./helper.js');



///--- Globals

var e = helper.e;
var entryStream = helper.entryStream;
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
