// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var lib = require('../../lib');
var memraft = require('../memraft');
var nodeunitPlus = require('nodeunit-plus');
var vasync = require('vasync');

// All the actual tests are here...
var raftRequestVoteTests = require('../share/raft_requestvote_tests.js');



///--- Globals

var before = nodeunitPlus.before;
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
        'peers': [ 'raft-1', 'raft-2' ]
    };

    //Need to "naturally" add some log entries, commit to state machines, etc.
    var raft;
    vasync.pipeline({
        funcs: [
            function init(o, subcb) {
                memraft.raft(opts, function (err, r) {
                    if (err) {
                        return (subcb(err));
                    }
                    raft = r;
                    return (subcb(null));
                });
            },
            function addEntries(o, subcb) {
                raft.appendEntries({
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
        self.raft = raft;
        //Set the leaderTimout low...
        raft.leaderTimeout = LOW_LEADER_TIMEOUT;
        raft.messageBus.blackholeUnknown = true;
        return (cb(err));
    });
});
