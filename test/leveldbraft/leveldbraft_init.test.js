// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var helper = require('../helper.js');
var leveldbraft = require('../leveldbraft');
var vasync = require('vasync');



// All the actual tests are here...
var raftInitTests = require('../share/raft_init_tests.js');



///--- Globals

var after = helper.after;
var before = helper.before;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});
var LOW_LEADER_TIMEOUT = 2;



///--- Setup/Teardown

before(function (cb) {
    assert.func(cb, 'cb');
    var self = this;
    leveldbraft.raft({
        'log': LOG,
        'id': 'raft-0',
        'peers': [ 'raft-1', 'raft-2' ],
        'dbName': 'raft_init_tests_db'
    }, function (err, raft) {
        self.raft = raft;
        raft.leaderTimeout = LOW_LEADER_TIMEOUT;
        cb(err);
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
