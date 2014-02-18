// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('../helper.js');
var lib = require('../../lib');
var memraft = require('../memory');
var nodeunitPlus = require('nodeunit-plus');
var vasync = require('vasync');

// All the actual tests are here...
var raftInitTests = require('../share/raft_init_tests.js');



///--- Globals

var before = nodeunitPlus.before;
var createClusterConfig = helper.createClusterConfig;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});
var LOW_LEADER_TIMEOUT = 2;



///--- Setup/Teardown

before(function (cb) {
    var self = this;
    memraft.raft({
        'log': LOG,
        'id': 'raft-0',
        'clusterConfig': createClusterConfig([ 'raft-0', 'raft-1', 'raft-2' ])
    }, function (err, r) {
        if (err) {
            return (cb(err));
        }
        self.raft = r;
        r.leaderTimeout = LOW_LEADER_TIMEOUT;
        return (cb(null));
    });
});
