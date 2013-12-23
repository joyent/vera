// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('./helper.js');
var TaskPipe = require('../lib/task_pipe');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memprops-test',
    stream: process.stdout
});



///--- Tests

test('single task', function (t) {
    var taskLog = [];
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp',
            'func': function (opts, cb) {
                assert.arrayOfString(opts);
                assert.ok(opts.length === 1);
                taskLog.push(opts[0]);
                setImmediate(cb);
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function appendOne(_, subcb) {
                taskPipe.append('0', subcb);
            },
            function check(_, subcb) {
                t.equal(1, taskLog.length);
                t.equal('0', taskLog[0]);
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


test('multiple tasks', function (t) {
    var taskLog = [];
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp',
            'func': function (opts, cb) {
                assert.arrayOfString(opts);
                assert.ok(opts.length === 1);
                taskLog.push(opts[0]);
                setImmediate(cb);
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function appendOne(_, subcb) {
                var done = 0;
                function tryEnd() {
                    ++done;
                    if (done === 4) {
                        subcb();
                    }
                }
                taskPipe.append('0', tryEnd);
                taskPipe.append('1', tryEnd);
                taskPipe.append('2', tryEnd);
                taskPipe.append('3', tryEnd);
            },
            function check(_, subcb) {
                t.equal(4, taskLog.length);
                assert.deepEqual([ '0', '1', '2', '3' ], taskLog);
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


test('consume multiple', function (t) {
    var taskLog = [];
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp',
            'func': function (opts, cb) {
                assert.arrayOfString(opts);
                assert.ok(opts.length === 2);
                taskLog.push(opts);
                setImmediate(cb);
            },
            'choose': function (arr) {
                return (2);
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function appendOne(_, subcb) {
                var done = 0;
                function tryEnd() {
                    ++done;
                    if (done === 4) {
                        subcb();
                    }
                }
                //The only reason this test works is that multiple are enqueued
                // during the same tick.
                taskPipe.append('0', tryEnd);
                taskPipe.append('1', tryEnd);
                taskPipe.append('2', tryEnd);
                taskPipe.append('3', tryEnd);
            },
            function check(_, subcb) {
                t.equal(2, taskLog.length);
                assert.deepEqual([ [ '0', '1'], ['2', '3' ] ], taskLog);
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


test('chain one', function (t) {
    var taskLog = [];
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp-1',
            'func': function (opts, cb) {
                assert.arrayOfString(opts);
                t.equal(1, opts.length);
                taskLog.push(opts[0]);
                cb();
            }
        }, {
            'name': 'tp-2',
            'func': function (opts, cb) {
                assert.arrayOfString(opts);
                t.equal(1, opts.length);
                taskLog.push(opts[0]);
                cb();
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function appendOne(_, subcb) {
                taskPipe.append('0', subcb);
            },
            function check(_, subcb) {
                t.deepEqual(['0', '0'], taskLog);
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


//Unfortunately, this test works by timing.  There's a possibility that it will
// break at some point.  If/when it does, we'll have to rewrite.
test('chain multiple', function (t) {
    var taskLog = [];
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp-1',
            'func': function (opts, cb) {
                assert.arrayOfString(opts);
                setTimeout(function () {
                    taskLog.push(opts.map(function (x) { return ('1-' + x); }));
                    cb();
                }, 100);
            },
            'choose': function (arr) {
                return (2);
            }
        }, {
            'name': 'tp-2',
            'func': function (opts, cb) {
                assert.arrayOfString(opts);
                setTimeout(function () {
                    taskLog.push(opts.map(function (x) { return ('2-' + x); }));
                    cb();
                }, 70);
            },
            'choose': function (arr) {
                return (1);
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function appendOne(_, subcb) {
                var done = 0;
                function tryEnd() {
                    ++done;
                    if (done === 5) {
                        subcb();
                    }
                }
                taskPipe.append('0', tryEnd);
                taskPipe.append('1', tryEnd);
                taskPipe.append('2', tryEnd);
                taskPipe.append('3', tryEnd);
                taskPipe.append('4', tryEnd);
            },
            function check(_, subcb) {
                //Explanation:
                t.deepEqual([
                    //100 ms: 0 and 1 from one, enqueue both to two.
                    [ '1-0', '1-1' ],
                    //170 ms: 0 consumed from two.
                    [ '2-0' ],
                    //200 ms: 2 and 3 from one, enqueue both to two.
                    [ '1-2', '1-3' ],
                    //240 ms: 1 consumed from two.
                    [ '2-1' ],
                    //300 ms: 4 from one, enqueue to two.
                    [ '1-4' ],
                    //310 ms: 2 consumed from two.
                    [ '2-2' ],
                    //380 ms: 3 consumed from two.
                    [ '2-3' ],
                    //450 ms: 4 consumed from two.
                    [ '2-4' ]
                ], taskLog);
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


test('chain break', function (t) {
    var reachedTwo = false;
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp-1',
            'func': function (opts, cb) {
                var e = new Error('ahhhhhhh!');
                e.name = 'OneError';
                cb(e);
            }
        }, {
            'name': 'tp-2',
            'func': function (opts, cb) {
                reachedTwo = true;
                cb();
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function appendOne(_, subcb) {
                taskPipe.append('0', subcb);
            }
        ]
    }, function (err) {
        if (!err) {
            t.fail('should have thrown an error!');
        }
        t.equal('OneError', err.name);
        t.ok(reachedTwo === false);
        t.done();
    });
});


test('setImmediate callback', function (t) {
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp-1',
            'func': function (opts, cb) {
                setImmediate(cb);
            }
        }, {
            'name': 'tp-2',
            'func': function (opts, cb) {
                setImmediate(cb);
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function append(_, subcb) {
                taskPipe.append('0', subcb);
            },
            function appendAgain(_, subcb) {
                taskPipe.append('0', subcb);
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('immediate callback', function (t) {
    var taskPipe = new TaskPipe({
        'tasks': [ {
            'name': 'tp-1',
            'func': function (opts, cb) {
                cb();
            }
        }, {
            'name': 'tp-2',
            'func': function (opts, cb) {
                cb();
            }
        } ]
    });
    vasync.pipeline({
        args: {},
        funcs: [
            function append(_, subcb) {
                taskPipe.append('0', subcb);
            },
            function appendAgain(_, subcb) {
                taskPipe.append('0', subcb);
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
