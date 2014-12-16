/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var helper = require('../helper');
var lib = require('../../lib');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var entryStream = helper.entryStream();
var LOW_LEADER_TIMEOUT = 2;



///--- Helpers

//See lib/raft.js#sendAppendEntries
function ae(req) {
    req.operation = req.operation || 'appendEntries';
    req.term = req.term === undefined ? 0 : req.term;
    req.leaderId = req.leaderId || 'raft-1';
    req.entries = req.entries || entryStream([ 0, 0 ]);
    req.commitIndex = req.commitIndex === undefined ? 0 : req.commitIndex;
    return (req);
}



///--- Tests

test('initial heartbeat (empty append, empty follower)', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function checkInitial(_, subcb) {
                var r = self.raft;
                t.ok(self.raft);
                t.equal(LOW_LEADER_TIMEOUT, r.leaderTimeout);
                t.equal(0, r.currentTerm());
                t.equal(undefined, r.leaderId);
                t.equal('follower', r.state);
                t.equal(1, r.clog.nextIndex);
                t.equal(0, r.stateMachine.commitIndex);
                t.equal(undefined, r.stateMachine.data);
                helper.readClog(r.clog, function (err, entries) {
                    t.equal(1, entries.length);
                    subcb();
                });
            },
            function append(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({}), function (err, res) {
                    t.ok(res);
                    t.equal(0, res.term);
                    t.ok(res.success);

                    t.equal(0, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(1, r.clog.nextIndex);
                    t.equal(0, r.stateMachine.commitIndex);
                    t.equal(undefined, r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(1, entries.length);
                        subcb();
                    });
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


test('first append, first commit', function (t) {
    var self = this;
    vasync.pipeline({
        arg: {},
        funcs: [
            function append(_, subcb) {
                var r = self.raft;
                t.equal(LOW_LEADER_TIMEOUT, r.leaderTimeout);
                r.appendEntries(ae({
                    'entries': entryStream([ 0, 0, 1, 0 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(0, res.term);
                    t.ok(res.success);

                    t.equal(0, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(2, r.clog.nextIndex);
                    t.equal(0, r.stateMachine.commitIndex);
                    t.equal(undefined, r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(2, entries.length);
                        t.equal('command-1-0', entries[1].command);
                        subcb();
                    });
                });
            },
            function commit(_, subcb) {
                var r = self.raft;
                r.appendEntries(ae({
                    'commitIndex': 1,
                    'entries': entryStream([ 0, 0, 1, 0 ])
                }), function (err, res) {
                    t.ok(res);
                    t.equal(0, res.term);
                    t.ok(res.success);

                    t.equal(0, r.currentTerm());
                    t.ok(r.leaderTimeout !== LOW_LEADER_TIMEOUT);
                    t.equal('raft-1', r.leaderId);
                    t.equal('follower', r.state);
                    t.equal(2, r.clog.nextIndex);
                    t.equal(1, r.stateMachine.commitIndex);
                    t.equal('command-1-0', r.stateMachine.data);
                    helper.readClog(r.clog, function (err2, entries) {
                        t.equal(2, entries.length);
                        t.equal('command-1-0', entries[1].command);
                        subcb();
                    });
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
