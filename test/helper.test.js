/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var helper = require('./helper.js');
var test = require('nodeunit-plus').test;



///--- Globals

var createClusterConfig = helper.createClusterConfig;
var e = helper.e();
var entryStream = helper.entryStream();



///--- Tests

test('createClusterConfig', function (t) {
    t.deepEqual({ 'current': { '<string>': { 'voting': true } } },
                createClusterConfig('<string>'));
    t.deepEqual({ 'current': { '<string>': { 'voting': true } } },
                createClusterConfig({ '<string>': { 'voting': true }}));
    t.deepEqual(
        {
            'current': {
                'foo': { 'voting': true },
                'bar': { 'voting': true },
                'baz': { 'voting': true }
            }
        },
        createClusterConfig([ 'foo', 'bar', 'baz' ]));
    t.done();
});

test('e', function (t) {
    var e1 = e(0, 1);
    t.ok(e1);
    t.equal(0, e1.index);
    t.equal(1, e1.term);
    t.deepEqual({
        'to': 'raft',
        'execute': 'configure',
        'cluster': { 'current': {} }
    }, e1.command);

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
            {
                'index': 0,
                'term': 0, 'command': {
                    'to': 'raft',
                    'execute': 'configure',
                    'cluster': { 'current': {} }
                }
            },
            { 'index': 1, 'term': 2, 'command': 'command-1-2'},
            { 'index': 2, 'term': 4, 'command': 'command-2-4'}
        ], entries);
        t.done();
    });
});
