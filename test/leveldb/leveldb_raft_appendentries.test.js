// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('../helper.js');
var lib = require('../../lib');
var leveldbraft = require('../leveldb');
var nodeunitPlus = require('nodeunit-plus');
var vasync = require('vasync');

// All the actual tests are here...
var raftAppendEntriesTests = require('../share/raft_appendentries_tests.js');

///--- Globals

var after = nodeunitPlus.after;
var before = nodeunitPlus.before;
var createClusterConfig = helper.createClusterConfig;
var memstream = lib.memstream;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});
var LOW_LEADER_TIMEOUT = 2;



///--- Setup/Teardown

before(function (cb) {
    var self = this;

    var clusterConfig = createClusterConfig([ 'raft-0', 'raft-1', 'raft-2' ]);
    var opts = {
        'log': LOG,
        'id': 'raft-0',
        'clusterConfig': clusterConfig,
        'dbName': 'raft_appendentries_tests_db'
    };

    var e = helper.e(clusterConfig);
    self.e = e;
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
                    'entries': memstream([
                        e(0, 0),
                        e(1, 1),
                        e(2, 2),
                        e(3, 3)
                    ]),
                    'commitIndex': 2
                }, subcb);
            },
            function requestVote(o, subcb) {
                self.raft.requestVote({
                    'operation': 'requestVote',
                    'candidateId': 'raft-1',
                    'term': 3,
                    'lastLogTerm': 3,
                    'lastLogIndex': 3
                }, subcb);
            },
            //To get the leader set.
            function assertLeader(o, subcb) {
                self.raft.appendEntries({
                    'operation': 'appendEntries',
                    'term': 3,
                    'leaderId': 'raft-1',
                    'entries': memstream([
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
