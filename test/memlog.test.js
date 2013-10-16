// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('./helper.js');
var MemLog = require('../lib/memlog');
var vasync = require('vasync');

///--- Globals

var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'debug'),
    name: 'checker',
    stream: process.stdout
});



///--- Helpers

function e(index, term) {
    return ({
        'index': index,
        'term': term,
        'data': 'data-' + index + '-' + term
    });
}


///--- Tests

test('add one', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append One
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0)
                ]
            }, subcb);
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


test('add two to begin with', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append Two
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0)
                ]
            }, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 2);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 0), ml.clog[1]);
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


test('add one, then two', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append One
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0)
                ]
            }, subcb);
        },
        //Append 2 more
        function (_, subcb) {
            ml.append({
                prevIndex: 0,
                prevTerm: 0,
                entries: [
                    e(1, 3),
                    e(2, 3)
                ]
            }, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 3);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 3), ml.clog[1]);
            t.deepEqual(e(2, 3), ml.clog[2]);
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


test('add two, then two more', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append Two
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0)
                ]
            }, subcb);
        },
        function (_, subcb) {
            ml.append({
                prevIndex: 1,
                prevTerm: 0,
                entries: [
                    e(2, 1),
                    e(3, 1)
                ]
            }, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 4);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 0), ml.clog[1]);
            t.deepEqual(e(2, 1), ml.clog[2]);
            t.deepEqual(e(3, 1), ml.clog[3]);
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


test('slices', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append Two
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0),
                    e(2, 1),
                    e(3, 1)
                ]
            }, subcb);
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


test('truncate off one', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append Two
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0)
                ]
            }, subcb);
        },
        //Truncate off prev
        function (_, subcb) {
            ml.truncate(ml.clog.length - 1, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 1);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.ok(ml.prevIndex === 0);
            t.ok(ml.prevTerm === 0);
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


test('truncate to 0', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append Two
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0)
                ]
            }, subcb);
        },
        //Truncate to 0
        function (_, subcb) {
            ml.truncate(0, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 0);
            t.ok(ml.prevIndex === null);
            t.ok(ml.prevTerm === null);
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


test('truncate to 0, add one', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append Two
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0)
                ]
            }, subcb);
        },
        //Truncate to 0
        function (_, subcb) {
            ml.truncate(0, subcb);
        },
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0)
                ]
            }, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 2);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 0), ml.clog[1]);
            t.ok(ml.prevIndex === 1);
            t.ok(ml.prevTerm === 0);
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


test('truncate to middle, add two', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append Four
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0),
                    e(2, 0),
                    e(3, 0)
                ]
            }, subcb);
        },
        //Truncate two
        function (_, subcb) {
            ml.truncate(2, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 2);
            t.deepEqual(e(0, 0), ml.clog[0]);
            t.deepEqual(e(1, 0), ml.clog[1]);
            t.ok(ml.prevIndex === 1);
            t.ok(ml.prevTerm === 0);
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
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append One
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 5)
                ]
            }, subcb);
        },
        function (_, subcb) {
            t.ok(ml.clog.length === 1);
            t.deepEqual(e(0, 5), ml.clog[0]);
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


test('prevIndex mismatch', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append One
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0)
                ]
            }, subcb);
        },
        function (_, subcb) {
            ml.append({
                prevIndex: 1,
                prevTerm: 0,
                entries: [
                    e(1, 0)
                ]
            }, subcb);
        }
    ];
    vasync.pipeline({
        funcs: funcs
    }, function (err) {
        if (!err || err.name !== 'IndexMismatchError') {
            t.fail('should have thrown an IndexMismatchError');
        }
        t.done();
    });
});


test('prevTerm mismatch', function (t) {
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append One
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0)
                ]
            }, subcb);
        },
        function (_, subcb) {
            ml.append({
                prevIndex: 0,
                prevTerm: 1,
                entries: [
                    e(1, 0)
                ]
            }, subcb);
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
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append One
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 0),
                    e(3, 0),
                    e(2, 0),
                    e(4, 0)
                ]
            }, subcb);
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
    var ml = new MemLog({
        'log': LOG
    });
    var funcs = [
        //Append One
        function (_, subcb) {
            ml.append({
                prevIndex: null,
                prevTerm: null,
                entries: [
                    e(0, 0),
                    e(1, 1),
                    e(2, 2),
                    e(3, 1),
                    e(4, 2)
                ]
            }, subcb);
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
