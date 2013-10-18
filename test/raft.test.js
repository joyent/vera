// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var helper = require('./helper.js');
var fs = require('fs');
var memlib = require('./raft');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'raft-test',
    stream: process.stdout
});



///--- Helpers

function checkInitalRaft(raft) {
    assert.ok(raft.id, 'raft.id');
    assert.ok(raft.log, 'raft.log');
    assert.equal(undefined, raft.leaderId);
    assert.number(raft.leaderTimeout);
    assert.equal(0, raft.currentTerm);
    assert.equal(undefined, raft.votedFor);
    assert.arrayOfString(raft.peers);
    assert.ok(raft.peers.indexOf(raft.id) === -1);
    assert.object(raft.clog);
    assert.object(raft.stateMachine);
    assert.object(raft.messageBus);
    assert.ok(Object.keys(raft.outstandingMessages).length === 0);
    assert.equal('follower', raft.state);
}


///--- Tests

test('init mem cluster of 3', function (t) {
    vasync.pipeline({
        arg: {},
        funcs: [
            function (_, subcb) {
                var opts = {
                    'log': LOG,
                    'size': 3
                };
                memlib.cluster(opts, function (err, cluster) {
                    _.cluster = cluster;
                    subcb();
                });
            },
            function (_, subcb) {
                var c = _.cluster;
                assert.object(c.messageBus, 'c.messageBus');
                assert.object(c.peers, 'c.peers');
                Object.keys(c.peers).forEach(function (p) {
                    checkInitalRaft(c.peers[p]);
                });
                subcb();
            }
        ]
    }, function (err) {
        t.done();
    });
});
