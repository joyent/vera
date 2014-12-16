/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var buffertools = require('buffertools');
var lib = require('../../lib/leveldb');
var test = require('nodeunit-plus').test;



///--- Helpers

function runLogKeyTest(t, i) {
    var enc = lib.key.log(i);
    var dec = lib.key.logDecode(enc);
    t.equal(i, dec);
}



///--- Tests

test('property key', function (t) {
    var s = 'foobarbaz';
    var enc = lib.key.property(s);
    var dec = lib.key.propertyDecode(enc);
    t.equal(s, dec);
    t.done();
});


test('internal property key', function (t) {
    var s = 'foobarbaz';
    var enc = lib.key.internalProperty(s);
    var dec = lib.key.propertyDecode(enc);
    t.equal(s, dec);
    t.done();
});


test('log key', function (t) {
    runLogKeyTest(t, 0);
    runLogKeyTest(t, 1);
    runLogKeyTest(t, 4294967295);
    runLogKeyTest(t, 4294967296);
    //Javascript's max integer - 1.
    runLogKeyTest(t, 9007199254740991);
    //Javascript's max integer.
    assert.throws(function () {
        runLogKeyTest(t, 9007199254740992);
    });
    //Javascript's max integer + 1 (an invalid integer).
    assert.throws(function () {
        runLogKeyTest(t, 9007199254740993);
    });
    t.done();
});


test('log key ordering', function (t) {
    var a = [9007199254740991, 2, 1, 4294967295, 0,
             9007199254740991, 4294967296];
    var a_sorted = [0, 1, 2, 4294967295, 4294967296,
                    9007199254740991, 9007199254740991];
    var c = a.map(function (n) { return (lib.key.log(n)); });
    //Compare comes from buffertools
    c.sort(function (l, r) { return (l.compare(r)); });
    t.equal(a.length, c.length);
    for (var i = 0; i < a.length; ++i) {
        t.equal(a_sorted[i], lib.key.logDecode(c[i]));
    }
    t.done();
});
