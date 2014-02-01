// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var memStream = require('../../lib').memStream;
var test = require('nodeunit-plus').test;
var StateMachine = require('./statemachine');
var vasync = require('vasync');



///--- Globals

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'statemachine-test',
    stream: process.stdout
});



///--- Tests

test('statemachine init and execute one', function (t) {
    var sm;
    var funcs = [
        function (_, subcb) {
            sm = new StateMachine({ 'log': LOG });
            sm.on('ready', subcb);
        },
        function (_, subcb) {
            t.equal(undefined, sm.data);
            t.equal(0, sm.commitIndex);
            subcb();
        },
        function (_, subcb) {
            sm.execute(memStream([
                { 'index': 1, 'command': 'one' }
            ]), function (err) {
                t.equal('one', sm.data);
                t.equal(1, sm.commitIndex);
                subcb(err);
            });
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('statemachine execute many', function (t) {
    var sm;
    var funcs = [
        function (_, subcb) {
            sm = new StateMachine({ 'log': LOG });
            sm.on('ready', subcb);
        },
        function (_, subcb) {
            sm.execute(memStream([
                { 'index': 1, 'command': 'one' },
                { 'index': 2, 'command': 'two' },
                { 'index': 3, 'command': 'three' },
                { 'index': 4, 'command': 'four' },
                { 'index': 5, 'command': 'five' },
                { 'index': 6, 'command': 'six' }
            ]), function (err) {
                t.equal('six', sm.data);
                t.equal(6, sm.commitIndex);
                subcb(err);
            });
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('statemachine first out of order', function (t) {
    var sm;
    var funcs = [
        function (_, subcb) {
            sm = new StateMachine({ 'log': LOG });
            sm.on('ready', subcb);
        },
        function (_, subcb) {
            sm.execute(memStream([
                { 'index': 2, 'command': 'two' }
            ]), subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (!err) {
            t.fail('should have failed with an error.');
        }
        t.equal('InternalError', err.name);
        t.done();
    });
});


test('statemachine middle out of order', function (t) {
    var sm;
    var funcs = [
        function (_, subcb) {
            sm = new StateMachine({ 'log': LOG });
            sm.on('ready', subcb);
        },
        function (_, subcb) {
            sm.execute(memStream([
                { 'index': 1, 'command': 'one' },
                { 'index': 2, 'command': 'two' },
                { 'index': 4, 'command': 'four' },
                { 'index': 3, 'command': 'three' }
            ]), subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (!err) {
            t.fail('should have failed with an error.');
        }
        t.equal('InternalError', err.name);
        t.done();
    });
});


test('clone', function (t) {
    var sm;
    var funcs = [
        function (_, subcb) {
            sm = new StateMachine({ 'log': LOG });
            sm.on('ready', subcb);
        },
        function (_, subcb) {
            var s = sm.snapshot();
            t.equal(0, s.commitIndex);
            t.equal(undefined, s.data);
            subcb();
        },
        function (_, subcb) {
            sm.execute(memStream([
                { 'index': 1, 'command': 'one' },
                { 'index': 2, 'command': 'two' }
            ]), subcb);
        },
        function (_, subcb) {
            var s = sm.snapshot();
            t.equal(2, s.commitIndex);
            t.equal('two', s.data);
            subcb();
        },
        function (_, subcb) {
            var smClone = sm.from(sm.snapshot());
            smClone.on('ready', function () {
                t.equal(2, smClone.commitIndex);
                t.equal('two', smClone.data);
                smClone.execute(memStream([
                    { 'index': 3, 'command': 'three' },
                    { 'index': 4, 'command': 'four' }
                ]), function (err) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.equal(4, smClone.commitIndex);
                    t.equal('four', smClone.data);
                    subcb();
                });
            });
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
