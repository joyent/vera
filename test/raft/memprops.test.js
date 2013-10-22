// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var MemProps = require('./memprops');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
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
                props = new MemProps({ 'log': LOG });
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
