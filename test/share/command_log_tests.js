// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var helper = require('../helper.js');
var lib = require('../../lib');
var vasync = require('vasync');



///--- Globals

var e = helper.e;
var entryStream = helper.entryStream;
var memStream = lib.memStream;
var test = helper.test;



///--- Helpers

function readStream(s, cb) {
    var res = [];
    s.on('readable', function () {
        var d;
        while (null !== (d = s.read())) {
            res.push(d);
        }
    });
    s.once('end', function () {
        return (cb(null, res));
    });
}


///--- Tests

test('consistency check on 0, success', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([ 0, 0 ])
            }, subcb);
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 1);
            t.deepEqual(e(0, 0), self.clog.clog[0]);
            t.equal(1, self.clog.nextIndex);
            self.clog.slice(0, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.ok(entries.length === 1);
                    t.deepEqual([ e(0, 0) ], entries);
                    subcb();
                });
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([ 0, 1 ])
            }, subcb);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                //No index, so append!
                'entries': memStream([ { 'term': 2,
                                         'command': 'command-1-2' } ])
            }, function (err, entry) {
                if (err) {
                    subcb(err);
                    return;
                }
                t.equal(1, entry.index);
                t.deepEqual(self.clog.last(), entry);
                t.equal(2, self.clog.nextIndex);
                subcb();
            });
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                //No index, so append!
                'entries': memStream([ { 'term': 2,
                                         'command': 'command-2-2' } ])
            }, function (err, entry) {
                t.equal(2, entry.index);
                t.deepEqual(self.clog.last(), entry);
                subcb();
            });
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 3);
            t.deepEqual(e(0, 0), self.clog.clog[0]);
            t.deepEqual(e(1, 2), self.clog.clog[1]);
            t.deepEqual(e(2, 2), self.clog.clog[2]);
            t.equal(3, self.clog.nextIndex);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': entryStream([
                    0, 0,
                    1, 1,
                    2, 1
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 3);
            t.deepEqual(e(0, 0), self.clog.clog[0]);
            t.deepEqual(e(1, 1), self.clog.clog[1]);
            t.deepEqual(e(2, 1), self.clog.clog[2]);
            t.equal(3, self.clog.nextIndex);
            self.clog.slice(1, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.ok(entries.length === 2);
                    t.deepEqual([ e(1, 1), e(2, 1) ], entries);
                    subcb();
                });
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    2, 1,
                    3, 1
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.slice(0, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.ok(entries.length === 4);
                    t.deepEqual(e(0, 0), self.clog.clog[0]);
                    t.deepEqual(e(1, 0), self.clog.clog[1]);
                    t.deepEqual(e(2, 1), self.clog.clog[2]);
                    t.deepEqual(e(3, 1), self.clog.clog[3]);
                    subcb();
                });
            });
        },
        function (_, subcb) {
            self.clog.slice(1, 2, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.ok(entries.length === 1);
                    t.deepEqual(e(1, 0), entries[0]);
                    subcb();
                });
            });
        },
        function (_, subcb) {
            self.clog.slice(1, 10, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.ok(entries.length === 3);
                    t.deepEqual(e(1, 0), entries[0]);
                    t.deepEqual(e(2, 1), entries[1]);
                    t.deepEqual(e(3, 1), entries[2]);
                    subcb();
                });
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    2, 0,
                    3, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    1, 0,
                    2, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 4);
            t.deepEqual(e(0, 0), self.clog.clog[0]);
            t.deepEqual(e(1, 0), self.clog.clog[1]);
            t.deepEqual(e(2, 0), self.clog.clog[2]);
            t.deepEqual(e(3, 0), self.clog.clog[3]);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    2, 0,
                    3, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': entryStream([
                    0, 0,
                    1, 1
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 2);
            t.deepEqual(e(0, 0), self.clog.clog[0]);
            t.deepEqual(e(1, 1), self.clog.clog[1]);
            t.equal(2, self.clog.nextIndex);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    2, 0,
                    3, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 3,
                'entries': entryStream([
                    1, 0,
                    2, 1,
                    3, 3
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 4);
            t.deepEqual(e(0, 0), self.clog.clog[0]);
            t.deepEqual(e(1, 0), self.clog.clog[1]);
            t.deepEqual(e(2, 1), self.clog.clog[2]);
            t.deepEqual(e(3, 3), self.clog.clog[3]);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    2, 0,
                    3, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.slice(1, 4, function (e1, entries) {
                t.ok(e1 === null);
                self.stateMachine.execute(entries, function (e2) {
                    t.equal(3, self.stateMachine.commitIndex);
                    return (subcb(e2));
                });
            });
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 3,
                'entries': entryStream([
                    1, 0,
                    2, 1,
                    3, 3
                ])
            }, subcb);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    2, 0,
                    3, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.slice(1, 4, function (e1, entries) {
                t.ok(e1 === null);
                self.stateMachine.execute(entries, function (e2) {
                    t.equal(3, self.stateMachine.commitIndex);
                    return (subcb(e2));
                });
            });
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': entryStream([
                    1, 0,
                    2, 0,
                    3, 1
                ])
            }, subcb);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    2, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 3,
                'entries': entryStream([
                    1, 0,
                    2, 3,
                    3, 3
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 4);
            t.deepEqual(e(0, 0), self.clog.clog[0]);
            t.deepEqual(e(1, 0), self.clog.clog[1]);
            t.deepEqual(e(2, 3), self.clog.clog[2]);
            t.deepEqual(e(3, 3), self.clog.clog[3]);
            t.equal(4, self.clog.nextIndex);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': memStream([ { 'term': 5,
                                         'command': 'command-1-5' } ])
            }, subcb);
        },
        function (_, subcb) {
            t.ok(self.clog.clog.length === 2);
            t.deepEqual(e(1, 5), self.clog.clog[1]);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    1, 1,
                    2, 1
                ])
            }, subcb);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 0,
                    3, 0,
                    2, 0,
                    4, 0
                ])
            }, subcb);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    0, 0,
                    1, 1,
                    2, 2,
                    3, 1,
                    4, 2
                ])
            }, subcb);
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
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': entryStream([
                    5, 5,
                    6, 6
                ])
            }, subcb);
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


test('term later than last entry', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 3,
                'entries': entryStream([
                    0, 0,
                    1, 5
                ])
            }, subcb);
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


test('commit index later than last entry', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 2,
                'term': 5,
                'entries': entryStream([
                    0, 0,
                    1, 5
                ])
            }, subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs,
        arg: {}
    }, function (err) {
        if (!err || err.name !== 'InvalidIndexError') {
            t.fail('should have thrown an TermMismatchError');
        }
        t.done();
    });
});
