// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var helper = require('../helper.js');
var lib = require('../../lib');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var e = helper.e();
var entryStream = helper.entryStream();
var configEntry = helper.configEntry;
var createClusterConfig = helper.createClusterConfig;
var memstream = lib.memstream;
var readClog = helper.readClog;
var readStream = helper.readStream;



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
            t.equal(1, self.clog.nextIndex);
            t.deepEqual(e(0, 0), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 1);
                t.deepEqual(e(0, 0), clog[0]);
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
                'entries': memstream([ { 'term': 2,
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
                'entries': memstream([ { 'term': 2,
                                         'command': 'command-2-2' } ])
            }, function (err, entry) {
                t.equal(2, entry.index);
                t.deepEqual(self.clog.last(), entry);
                subcb();
            });
        },
        function (_, subcb) {
            t.equal(3, self.clog.nextIndex);
            t.deepEqual(e(2, 2), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 3);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 2), clog[1]);
                t.deepEqual(e(2, 2), clog[2]);
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
            t.equal(3, self.clog.nextIndex);
            t.deepEqual(e(2, 1), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 3);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 1), clog[1]);
                t.deepEqual(e(2, 1), clog[2]);
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
                    t.deepEqual(e(0, 0), entries[0]);
                    t.deepEqual(e(1, 0), entries[1]);
                    t.deepEqual(e(2, 1), entries[2]);
                    t.deepEqual(e(3, 1), entries[3]);
                    subcb();
                });
            });
        },
        function (_, subcb) {
            self.clog.slice(1, 2, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.equal(1, entries.length);
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
        },
        function (_, subcb) {
            self.clog.slice(1, 1, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.ok(entries.length === 0);
                    subcb();
                });
            });
        },
        function (_, subcb) {
            self.clog.slice(1, -1, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.ok(entries.length === 0);
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
            t.equal(4, self.clog.nextIndex);
            t.deepEqual(e(3, 0), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 4);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 0), clog[1]);
                t.deepEqual(e(2, 0), clog[2]);
                t.deepEqual(e(3, 0), clog[3]);
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
            t.equal(2, self.clog.nextIndex);
            t.deepEqual(e(1, 1), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 2);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 1), clog[1]);
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
                    3, 0,
                    4, 0
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
                    3, 1
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(4, self.clog.nextIndex);
            t.deepEqual(e(3, 1), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.equal(4, clog.length);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 0), clog[1]);
                t.deepEqual(e(2, 1), clog[2]);
                t.deepEqual(e(3, 1), clog[3]);
                subcb();
            });
        },
        function (_, subcb) {
            self.clog.slice(0, 10, function (err, es) {
                readStream(es, function (err2, entries) {
                    t.equal(4, entries.length);
                    t.deepEqual(e(0, 0), entries[0]);
                    t.deepEqual(e(1, 0), entries[1]);
                    t.deepEqual(e(2, 1), entries[2]);
                    t.deepEqual(e(3, 1), entries[3]);
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
            t.equal(4, self.clog.nextIndex);
            t.deepEqual(e(3, 3), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 4);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 0), clog[1]);
                t.deepEqual(e(2, 3), clog[2]);
                t.deepEqual(e(3, 3), clog[3]);
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


test('add first, term > 0', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': memstream([ { 'term': 5,
                                         'command': 'command-1-5' } ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(2, self.clog.nextIndex);
            t.deepEqual(e(1, 5), self.clog.last());
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 2);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 5), clog[1]);
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


test('entry term later than last entry', function (t) {
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


///--- Cluster Config Tests

test('append reconfiguration', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 0,
                'entries': memstream([
                    { 'term': 1, //No index, so this is an append.
                      'command': {
                          'to': 'raft',
                          'execute': 'configure',
                          'cluster': createClusterConfig('raft-0', 0)
                      }
                    }
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(2, self.clog.nextIndex);
            t.deepEqual(configEntry(1, 1, createClusterConfig('raft-0', 0)),
                        self.clog.last());
            t.equal(1, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-0', 0);
            cc.clogIndex = 1;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 2);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(configEntry(1, 1, createClusterConfig('raft-0', 0)),
                            clog[1]);
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


test('reconfigure at beginning', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            var cc = createClusterConfig('raft-0');
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    configEntry(1, 1, cc),
                    e(2, 1),
                    e(3, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(4, self.clog.nextIndex);
            t.deepEqual(e(3, 1), self.clog.last());
            t.equal(1, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-0', 0);
            cc.clogIndex = 1;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 4);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(configEntry(1, 1, createClusterConfig('raft-0', 0)),
                            clog[1]);
                t.deepEqual(e(2, 1), clog[2]);
                t.deepEqual(e(3, 1), clog[3]);
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


test('reconfigure in middle', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            var cc = createClusterConfig('raft-0');
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    e(1, 1),
                    configEntry(2, 1, cc),
                    e(3, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(4, self.clog.nextIndex);
            t.deepEqual(e(3, 1), self.clog.last());
            t.equal(2, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-0', 0);
            cc.clogIndex = 2;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 4);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 1), clog[1]);
                t.deepEqual(configEntry(2, 1, createClusterConfig('raft-0', 0)),
                            clog[2]);
                t.deepEqual(e(3, 1), clog[3]);
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


test('reconfigure at end', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            var cc = createClusterConfig('raft-0');
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    e(1, 1),
                    e(2, 1),
                    configEntry(3, 1, cc)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(4, self.clog.nextIndex);
            t.deepEqual(configEntry(3, 1, createClusterConfig('raft-0', 0)),
                        self.clog.last());
            t.equal(3, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-0', 0);
            cc.clogIndex = 3;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 4);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 1), clog[1]);
                t.deepEqual(e(2, 1), clog[2]);
                t.deepEqual(configEntry(3, 1, createClusterConfig('raft-0', 0)),
                            clog[3]);
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


test('many reconfigures', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    configEntry(1, 1, createClusterConfig('raft-0')),
                    configEntry(2, 1, createClusterConfig('raft-1')),
                    e(3, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(3, 1),
                    configEntry(4, 1, createClusterConfig('raft-2'))
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    configEntry(4, 1, createClusterConfig('raft-2')),
                    configEntry(5, 1, createClusterConfig('raft-3')),
                    e(6, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(7, self.clog.nextIndex);
            t.deepEqual(e(6, 1), self.clog.last());
            t.equal(5, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-3', 4);
            cc.clogIndex = 5;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 7);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(configEntry(1, 1, createClusterConfig('raft-0', 0)),
                            clog[1]);
                t.deepEqual(configEntry(2, 1, createClusterConfig('raft-1', 1)),
                            clog[2]);
                t.deepEqual(e(3, 1), clog[3]);
                t.deepEqual(configEntry(4, 1, createClusterConfig('raft-2', 2)),
                            clog[4]);
                t.deepEqual(configEntry(5, 1, createClusterConfig('raft-3', 4)),
                            clog[5]);
                t.deepEqual(e(6, 1), clog[6]);
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


//Tests that a later reconfigure won't get overwritten by an append entries
// request whose set of entries occurs before
test('earlier config doesn\'t overwrite later', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    e(1, 1),
                    configEntry(2, 1, createClusterConfig('raft-0')),
                    e(3, 1),
                    configEntry(4, 1, createClusterConfig('raft-1')),
                    e(5, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(1, 1),
                    configEntry(2, 1, createClusterConfig('raft-0')),
                    e(3, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(6, self.clog.nextIndex);
            t.deepEqual(e(5, 1), self.clog.last());
            t.equal(4, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-1', 2);
            cc.clogIndex = 4;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 6);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 1), clog[1]);
                t.deepEqual(configEntry(2, 1, createClusterConfig('raft-0', 0)),
                            clog[2]);
                t.deepEqual(e(3, 1), clog[3]);
                t.deepEqual(configEntry(4, 1, createClusterConfig('raft-1', 2)),
                            clog[4]);
                t.deepEqual(e(5, 1), clog[5]);
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


test('truncate before config', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            var cc = createClusterConfig('raft-0');
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    e(1, 1),
                    configEntry(2, 1, cc),
                    e(3, 1),
                    e(4, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 2,
                'entries': memstream([
                    e(0, 0),
                    e(1, 2)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(2, self.clog.nextIndex);
            t.deepEqual(e(1, 2), self.clog.last());
            t.equal(0, self.clog.clusterConfigIndex);
            var cc = createClusterConfig();
            cc.clogIndex = 0;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 2);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 2), clog[1]);
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


test('truncate at latest config', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            var cc = createClusterConfig('raft-0');
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    e(1, 1),
                    configEntry(2, 1, cc),
                    e(3, 1),
                    e(4, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 2,
                'entries': memstream([
                    e(1, 1),
                    e(2, 2)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(3, self.clog.nextIndex);
            t.deepEqual(e(2, 2), self.clog.last());
            t.equal(0, self.clog.clusterConfigIndex);
            var cc = createClusterConfig();
            cc.clogIndex = 0;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 3);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 1), clog[1]);
                t.deepEqual(e(2, 2), clog[2]);
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


test('truncate using config as check', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            var cc = createClusterConfig('raft-0');
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    e(1, 1),
                    configEntry(2, 1, cc),
                    e(3, 1),
                    e(4, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            var cc = createClusterConfig('raft-0');
            self.clog.append({
                'commitIndex': 0,
                'term': 2,
                'entries': memstream([
                    configEntry(2, 1, cc),
                    e(3, 2)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(4, self.clog.nextIndex);
            t.deepEqual(e(3, 2), self.clog.last());
            t.equal(2, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-0', 0);
            cc.clogIndex = 2;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 4);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(e(1, 1), clog[1]);
                t.deepEqual(configEntry(2, 1, createClusterConfig('raft-0', 0)),
                            clog[2]);
                t.deepEqual(e(3, 2), clog[3]);
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


test('truncate many configs', function (t) {
    var self = this;
    var funcs = [
        function (_, subcb) {
            //Verify initial config state.
            t.equal(0, self.clog.clusterConfigIndex);
            t.deepEqual({
                'clogIndex': 0,
                'current': {}
            }, self.clog.clusterConfig);
            subcb();
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 1,
                'entries': memstream([
                    e(0, 0),
                    configEntry(1, 1, createClusterConfig('raft-0')),
                    configEntry(2, 1, createClusterConfig('raft-1')),
                    configEntry(3, 1, createClusterConfig('raft-2')),
                    configEntry(4, 1, createClusterConfig('raft-3')),
                    configEntry(5, 1, createClusterConfig('raft-4')),
                    configEntry(6, 1, createClusterConfig('raft-5')),
                    configEntry(7, 1, createClusterConfig('raft-6')),
                    configEntry(8, 1, createClusterConfig('raft-7')),
                    e(9, 1)
                ])
            }, subcb);
        },
        function (_, subcb) {
            self.clog.append({
                'commitIndex': 0,
                'term': 2,
                'entries': memstream([
                    configEntry(3, 1, createClusterConfig('raft-2')),
                    e(4, 2)
                ])
            }, subcb);
        },
        function (_, subcb) {
            t.equal(5, self.clog.nextIndex);
            t.deepEqual(e(4, 2), self.clog.last());
            t.equal(3, self.clog.clusterConfigIndex);
            var cc = createClusterConfig('raft-2', 2);
            cc.clogIndex = 3;
            t.deepEqual(cc, self.clog.clusterConfig);
            readClog(self.clog, function (err, clog) {
                t.ok(clog.length === 5);
                t.deepEqual(e(0, 0), clog[0]);
                t.deepEqual(configEntry(1, 1, createClusterConfig('raft-0', 0)),
                            clog[1]);
                t.deepEqual(configEntry(2, 1, createClusterConfig('raft-1', 1)),
                            clog[2]);
                t.deepEqual(configEntry(3, 1, createClusterConfig('raft-2', 2)),
                            clog[3]);
                t.deepEqual(e(4, 2), clog[4]);
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
