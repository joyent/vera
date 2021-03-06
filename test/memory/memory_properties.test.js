/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var lib = require('../../lib/memory');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memprops-test',
    stream: process.stdout
});



///--- Tests

test('crud', function (t) {
    var props;
    vasync.pipeline({
        args: {},
        funcs: [
            function init(_, subcb) {
                props = new lib.Properties({ 'log': LOG });
                props.on('ready', subcb);
            },
            function write(_, subcb) {
                t.ok(props.get('foo') === undefined);
                t.ok(props.get('bar') === undefined);
                var p = { 'foo': 'fval', 'bar': 'bval'};
                props.write(p, subcb);
            },
            function read(_, subcb) {
                t.equal('fval', props.get('foo'));
                t.equal('bval', props.get('bar'));
                subcb();
            },
            function update(_, subcb) {
                var p = { 'bar': 'bval2'};
                props.write(p, subcb);
            },
            function checkUpdate(_, subcb) {
                t.equal('fval', props.get('foo'));
                t.equal('bval2', props.get('bar'));
                subcb();
            },
            function del(_, subcb) {
                props.delete('bar', subcb);
            },
            function checkDel(_, subcb) {
                t.equal('fval', props.get('foo'));
                t.ok(props.get('bar') === undefined);
                subcb();
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
