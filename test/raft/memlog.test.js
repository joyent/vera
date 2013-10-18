// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var MemLog = require('./memlog');
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
        'data': index === 0 ? 'noop' : 'data-' + index + '-' + term
    });
}



///--- Tests

test('consistency check on 0, success', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 1);
            t.deepEqual(e(0, 0), ml.clog[0]);
            ml.slice(0, function (err, entries) {
                t.ok(entries.length === 1);
                t.deepEqual([ e(0, 0) ], entries);
                subcb();
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


test('consistency check on 0, fail', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 1)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (!err) {
            t.fail('should have thrown an error...');
        }
        t.equal('TermMismatchError', err.name);
        t.done();
    });
});


test('append one at a time', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                //No index, so append!
                { 'term': 2, 'data': 'data-1-2' }
            ], function (err, entry) {
                if (err) {
                    subcb(err);
                    return;
                }
                t.equal(1, entry.index);
                t.deepEqual(ml.last(), entry);
                subcb();
            });
        },
        function (_, subcb) {
            ml.append([
                //No index, so append!
                { 'term': 2, 'data': 'data-2-2' }
            ], function (err, entry) {
                t.equal(2, entry.index);
                t.deepEqual(ml.last(), entry);
                subcb();
            });
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 3);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 2), ml.clog[1]);
            t.deepEqual(e(2, 2), ml.clog[2]);
            subcb();
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


test('add two success', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 1),
                e(2, 1)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 3);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 1), ml.clog[1]);
            t.deepEqual(e(2, 1), ml.clog[2]);
            ml.slice(1, function (err, entries) {
                t.ok(entries.length === 2);
                t.deepEqual([ e(1, 1), e(2, 1) ], entries);
                subcb();
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


test('slices', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 1),
                e(3, 1)
            ], subcb);
        },
        function (_, subcb) {
            ml.slice(0, function (err, entries) {
                t.ok(entries.length === 4);
                t.deepEqual(e(0, 0), ml.clog[0]);
                t.deepEqual(e(1, 0), ml.clog[1]);
                t.deepEqual(e(2, 1), ml.clog[2]);
                t.deepEqual(e(3, 1), ml.clog[3]);
                subcb();
            });
        },
        function (_, subcb) {
            ml.slice(1, 2, function (err, entries) {
                t.ok(entries.length === 1);
                t.deepEqual(e(1, 0), entries[0]);
                subcb();
            });
        },
        function (_, subcb) {
            ml.slice(1, 10, function (err, entries) {
                t.ok(entries.length === 3);
                t.deepEqual(e(1, 0), entries[0]);
                t.deepEqual(e(2, 1), entries[1]);
                t.deepEqual(e(3, 1), entries[2]);
                subcb();
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


test('idempotent, two in the middle.', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            ml.append([
                e(1, 0),
                e(2, 0)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 4);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 0), ml.clog[1]);
            t.deepEqual(e(2, 0), ml.clog[2]);
            t.deepEqual(e(3, 0), ml.clog[3]);
            subcb();
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


test('cause truncate from beginning', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 1)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 2);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 1), ml.clog[1]);
            subcb();
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


test('cause truncate in middle', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0),
                e(3, 0)
            ], subcb);
        },
        function (_, subcb) {
            ml.append([
                e(1, 0),
                e(2, 1),
                e(3, 3)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 4);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 0), ml.clog[1]);
            t.deepEqual(e(2, 1), ml.clog[2]);
            t.deepEqual(e(3, 3), ml.clog[3]);
            subcb();
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


test('cause replace end, add one', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 0),
                e(2, 0)
            ], subcb);
        },
        function (_, subcb) {
            ml.append([
                e(1, 0),
                e(2, 3),
                e(3, 3)
            ], subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 4);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 0), ml.clog[1]);
            t.deepEqual(e(2, 3), ml.clog[2]);
            t.deepEqual(e(3, 3), ml.clog[3]);
            subcb();
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


test('add first, term > 0', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                { 'term': 5, 'data': 'data-1-5' }
            ], subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 2);
            t.deepEqual(e(1, 5), ml.clog[1]);
            subcb();
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


test('term mismatch', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 0)
            ], subcb);
        },
        function (_, subcb) {
            ml.append([
                e(1, 1),
                e(2, 1)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (!err || err.name !== 'TermMismatchError') {
            t.fail('should have thrown an TermMismatchError');
        }
        t.done();
    });
});


test('indexes out of order', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 0),
                e(3, 0),
                e(2, 0),
                e(4, 0)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (!err || err.name !== 'InvalidIndexError') {
            t.fail('should have thrown an InvalidIndexError');
        }
        t.done();
    });
});


test('term out of order', function (t) {
    var ml;
    var funcs = [
        function (_, subcb) {
            ml = new MemLog({ 'log': LOG });
            ml.on('ready', subcb);
        },
        function (_, subcb) {
            ml.append([
                e(0, 0),
                e(1, 1),
                e(2, 2),
                e(3, 1),
                e(4, 2)
            ], subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (!err || err.name !== 'InvalidTermError') {
            t.fail('should have thrown an InvalidTermError');
        }
        t.done();
    });
});
