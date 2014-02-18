// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var memraft = require('../memory');
var nodeunitPlus = require('nodeunit-plus');
var vasync = require('vasync');

// All the actual tests are here...
var raftInstallSnapshotTests =
    require('../share/raft_install_snapshot_tests.js');


///--- Globals

var before = nodeunitPlus.before;
var createClusterConfig = helper.createClusterConfig;
var e = helper.e();
var entryStream = helper.entryStream();
var test = nodeunitPlus.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memlog-test',
    stream: process.stdout
});



///--- Setup/Teardown

before(function (cb) {
    var self = this;
    var peers = [ 'raft-0', 'raft-1' ];
    vasync.forEachParallel({
        'inputs': peers.map(function (p) {
            return ({
                'log': LOG,
                'id': p,
                'clusterConfig': createClusterConfig([ p ])
            });
        }),
        'func': memraft.raft
    }, function (err, res) {
        if (err) {
            return (cb(err));
        }
        self.oldRaft = res.operations[0].result;
        self.newRaft = res.operations[1].result;
        //Manually set the old raft to leader so that we can make client
        // requests.
        self.oldRaft.on('stateChange', function (state) {
            cb();
        });
        self.oldRaft.transitionToLeader();
    });
});



///--- Tests only for the memraft...

test('get snapshot', function (t) {
    var self = this;
    var oldRaft = self.oldRaft;
    var snapshotter = oldRaft.snapshotter;
    vasync.pipeline({
        funcs: [
            function (_, subcb) {
                snapshotter.getLatest(function (err, snapshot) {
                    if (err) {
                        return (subcb(err));
                    }
                    t.deepEqual({
                        'stateMachineData': {
                            'commitIndex': 0,
                            'data': undefined
                        },
                        'clogData': {
                            'clog': [
                                helper.e(createClusterConfig('raft-0'))(0, 0)
                            ],
                            'clusterConfigIndex': 0
                        }
                    }, snapshot);
                    subcb();
                });
            }
        ]
    }, function (err) {
        if (err) {
            t.fail(err);
        }
        t.done();
    });
});
