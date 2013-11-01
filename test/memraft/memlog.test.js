// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var MemLog = require('./memlog');
var StateMachine = require('./statemachine');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memlog-test',
    stream: process.stdout
});



///--- Helpers

function e(index, term) {
    return ({
        'index': index,
        'term': term,
        'command': index === 0 ? 'noop' : 'command-' + index + '-' + term
    });
}


function initMemLog() {
    return (function init(_, cb) {
        vasync.pipeline({
            funcs: [
                function initStateMachine(o, subcb) {
                    _.stateMachine = new StateMachine({ 'log': LOG });
                    _.stateMachine.on('ready', subcb);
                },
                function initMemLogHere(o, subcb) {
                    _.ml = new MemLog({ 'log': LOG,
                                        'stateMachine': _.stateMachine });
                    _.ml.on('ready', subcb);
                }
            ]
        }, function (err) {
            cb(err);
        });
    });
}



///--- Tests

test('consistency check on 0, success', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 1);
            t.deepEqual(e(0, 0), _.ml.clog[0]);
            t.equal(1, _.ml.nextIndex);
            _.ml.slice(0, function (err, entries) {
                t.ok(entries.length === 1);
                t.deepEqual([ e(0, 0) ], entries);
                subcb();
            });
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('consistency check on 0, fail', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 1)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (!err) {
            t.fail('should have thrown an error...');
        }
        t.equal('TermMismatchError', err.name);
        t.done();
    });
});


test('append one at a time', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                //No index, so append!
                { 'term': 2, 'command': 'command-1-2' }
            ], function (err, entry) {
                if (err) {
                    subcb(err);
                    return;
                }
                t.equal(1, entry.index);
                t.deepEqual(_.ml.last(), entry);
                t.equal(2, _.ml.nextIndex);
                subcb();
            });
        },
        function (_, subcb) {
            _.ml.append([
                //No index, so append!
                { 'term': 2, 'command': 'command-2-2' }
            ], function (err, entry) {
                t.equal(2, entry.index);
                t.deepEqual(_.ml.last(), entry);
                subcb();
            });
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 3);
            t.deepEqual(e(0, 0), _.ml.clog[0]);
            t.deepEqual(e(1, 2), _.ml.clog[1]);
            t.deepEqual(e(2, 2), _.ml.clog[2]);
            t.equal(3, _.ml.nextIndex);
            subcb();
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('add two success', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 1),
                e(2, 1)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 3);
            t.deepEqual(e(0, 0), _.ml.clog[0]);
            t.deepEqual(e(1, 1), _.ml.clog[1]);
            t.deepEqual(e(2, 1), _.ml.clog[2]);
            t.equal(3, _.ml.nextIndex);
            _.ml.slice(1, function (err, entries) {
                t.ok(entries.length === 2);
                t.deepEqual([ e(1, 1), e(2, 1) ], entries);
                subcb();
            });
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('slices', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 1),
                e(3, 1)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.slice(0, function (err, entries) {
                t.ok(entries.length === 4);
                t.deepEqual(e(0, 0), _.ml.clog[0]);
                t.deepEqual(e(1, 0), _.ml.clog[1]);
                t.deepEqual(e(2, 1), _.ml.clog[2]);
                t.deepEqual(e(3, 1), _.ml.clog[3]);
                subcb();
            });
        },
        function (_, subcb) {
            _.ml.slice(1, 2, function (err, entries) {
                t.ok(entries.length === 1);
                t.deepEqual(e(1, 0), entries[0]);
                subcb();
            });
        },
        function (_, subcb) {
            _.ml.slice(1, 10, function (err, entries) {
                t.ok(entries.length === 3);
                t.deepEqual(e(1, 0), entries[0]);
                t.deepEqual(e(2, 1), entries[1]);
                t.deepEqual(e(3, 1), entries[2]);
                subcb();
            });
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('idempotent, two in the middle.', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.append([
                e(1, 0),
                e(2, 0)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 4);
            t.deepEqual(e(0, 0), _.ml.clog[0]);
            t.deepEqual(e(1, 0), _.ml.clog[1]);
            t.deepEqual(e(2, 0), _.ml.clog[2]);
            t.deepEqual(e(3, 0), _.ml.clog[3]);
            subcb();
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('cause truncate from beginning', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 1)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 2);
            t.deepEqual(e(0, 0), _.ml.clog[0]);
            t.deepEqual(e(1, 1), _.ml.clog[1]);
            t.equal(2, _.ml.nextIndex);
            subcb();
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('cause truncate in middle', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.append([
                e(1, 0),
                e(2, 1),
                e(3, 3)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 4);
            t.deepEqual(e(0, 0), _.ml.clog[0]);
            t.deepEqual(e(1, 0), _.ml.clog[1]);
            t.deepEqual(e(2, 1), _.ml.clog[2]);
            t.deepEqual(e(3, 3), _.ml.clog[3]);
            subcb();
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('truncate before commit index', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.slice(1, 4, function (e1, entries) {
                t.ok(e1 === null);
                _.stateMachine.execute(entries, function (e2) {
                    t.equal(3, _.stateMachine.commitIndex);
                    return (subcb(e2));
                });
            });
        },
        function (_, subcb) {
            _.ml.append([
                e(1, 0),
                e(2, 1),
                e(3, 3)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        t.ok(err !== null);
        t.equal('InternalError', err.name);
        t.done();
    });
});


test('truncate at commit index', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.slice(1, 4, function (e1, entries) {
                t.ok(e1 === null);
                _.stateMachine.execute(entries, function (e2) {
                    t.equal(3, _.stateMachine.commitIndex);
                    return (subcb(e2));
                });
            });
        },
        function (_, subcb) {
            _.ml.append([
                e(1, 0),
                e(2, 0),
                e(3, 1)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        t.ok(err !== null);
        t.equal('InternalError', err.name);
        t.done();
    });
});


test('cause replace end, add one', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.append([
                e(1, 0),
                e(2, 3),
                e(3, 3)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 4);
            t.deepEqual(e(0, 0), _.ml.clog[0]);
            t.deepEqual(e(1, 0), _.ml.clog[1]);
            t.deepEqual(e(2, 3), _.ml.clog[2]);
            t.deepEqual(e(3, 3), _.ml.clog[3]);
            t.equal(4, _.ml.nextIndex);
            subcb();
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('add first, term > 0', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                { 'term': 5, 'command': 'command-1-5' }
            ], subcb);
        },
        function (_, subcb) {
            t.ok(_.ml.clog.length === 2);
            t.deepEqual(e(1, 5), _.ml.clog[1]);
            subcb();
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});


test('term mismatch', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0)
            ], subcb);
        },
        function (_, subcb) {
            _.ml.append([
                e(1, 1),
                e(2, 1)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (!err || err.name !== 'TermMismatchError') {
            t.fail('should have thrown an TermMismatchError');
        }
        t.done();
    });
});


test('indexes out of order', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 0),
                e(3, 0),
                e(2, 0),
                e(4, 0)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (!err || err.name !== 'InvalidIndexError') {
            t.fail('should have thrown an InvalidIndexError');
        }
        t.done();
    });
});


test('term out of order', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(0, 0),
                e(1, 1),
                e(2, 2),
                e(3, 1),
                e(4, 2)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (!err || err.name !== 'InvalidTermError') {
            t.fail('should have thrown an InvalidTermError');
        }
        t.done();
    });
});


test('append past end', function (t) {
    var funcs = [
        initMemLog(),
        function (_, subcb) {
            _.ml.append([
                e(5, 5),
                e(6, 6)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (!err || err.name !== 'TermMismatchError') {
            t.fail('should have thrown an TermMismatchError');
        }
        t.done();
    });
});
