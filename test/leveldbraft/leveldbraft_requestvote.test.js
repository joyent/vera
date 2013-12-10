// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('../helper.js');
var lib = require('../../lib');
var leveldbraft = require('../leveldbraft');
var vasync = require('vasync');

// All the actual tests are here...
var raftRequestVoteTests = require('../share/raft_requestvote_tests.js');

///--- Globals

var after = helper.after;
var before = helper.before;
var e = helper.e;
var memStream = lib.memStream;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});
var LOW_LEADER_TIMEOUT = 2;



///--- Setup/Teardown

before(function (cb) {
    var self = this;

    var opts = {
        'log': LOG,
        'id': 'raft-0',
        'peers': [ 'raft-1', 'raft-2' ],
        'dbName': 'raft_requestvote_tests_db'
    };

    //Need to "naturally" add some log entries, commit to state machines, etc.
    vasync.pipeline({
        funcs: [
            function init(o, subcb) {
                leveldbraft.raft(opts, function (err, r) {
                    if (err) {
                        return (subcb(err));
                    }
                    self.raft = r;
                    return (subcb(null));
                });
            },
            function addEntries(o, subcb) {
                self.raft.appendEntries({
                    'operation': 'appendEntries',
                    'term': 3,
                    'leaderId': 'raft-1',
                    'entries': memStream([
                        e(0, 0, 'noop'),
                        e(1, 1, 'one'),
                        e(2, 2, 'two'),
                        e(3, 3, 'three')
                    ]),
                    'commitIndex': 2
                }, subcb);
            }
        ]
    }, function (err) {
        //Set the leaderTimout low...
        self.raft.leaderTimeout = LOW_LEADER_TIMEOUT;
        self.raft.messageBus.blackholeUnknown = true;
        return (cb(err));
    });
});


after(function (cb) {
    var self = this;
    vasync.pipeline({
        'funcs': [
            function closeLevelDb(_, subcb) {
                self.raft.clog.close(subcb);
            }
        ]
    }, function (err) {
        cb(err);
    });
});
