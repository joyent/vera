/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var lib = require('../../lib/memory');
var test = require('nodeunit-plus').test;
var vasync = require('vasync');



///--- Globals

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'messagebus-test',
    stream: process.stdout
});



///--- Helpers

function FakeRaft() {
    var self = this;
    self.requestVoteCalled = 0;
    self.appendEntriesCalled = 0;
}

FakeRaft.prototype.requestVote = function (req, cb) {
    var self = this;
    ++self.requestVoteCalled;
    setImmediate(cb);
};

FakeRaft.prototype.appendEntries = function (req, cb) {
    var self = this;
    ++self.appendEntriesCalled;
    setImmediate(cb);
};

FakeRaft.prototype.installSnapshot = function (req, cb) {
    var self = this;
    ++self.installSnapshot;
    setImmediate(cb);
};

function getTopology(n) {
    var peers = {};
    for (var i = 0; i < n; ++i) {
        peers['' + i] = new FakeRaft();
    }
    return (peers);
}


///--- Tests

test('request/reply', function (t) {
    var mb;
    var peers = getTopology(2);
    var funcs = [
        function init(_, subcb) {
            mb = new lib.MessageBus({ 'log': LOG, 'peers': peers });
            mb.on('ready', subcb);
        },
        function reqrep(_, subcb) {
            var messageId;
            var responseCalled = false;
            function onResponse(err, gMessageId, from, res) {
                t.equal(1, peers['0'].appendEntriesCalled);
                t.equal(messageId, gMessageId);
                t.equal('0', from);
                responseCalled = true;
            }

            t.equal(0, peers['0'].appendEntriesCalled);
            messageId = mb.send(
                'me', '0', { 'operation': 'appendEntries' }, onResponse);

            setImmediate(function () {
                t.equal(1, Object.keys(mb.messages).length);
                mb.tick(function () {
                    t.ok(responseCalled);
                    subcb();
                });
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


test('unknown peer', function (t) {
    var mb;
    var peers = getTopology(2);
    var funcs = [
        function init(_, subcb) {
            mb = new lib.MessageBus({ 'log': LOG, 'peers': peers });
            mb.on('ready', subcb);
        },
        function causeError(_, subcb) {
            var gMessageId;
            function onResponse(err, messageId, from) {
                t.ok(err);
                t.equal('InvalidPeerError', err.name);
                t.equal(0, Object.keys(mb.messages).length);
                t.equal(messageId, gMessageId);
                t.equal('2', from);
                return (subcb());
            }
            gMessageId =
                mb.send('me', '2', { 'operation': 'fake' }, onResponse);
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


test('partition and unpartition', function (t) {
    var mb;
    var peers = getTopology(2);
    var funcs = [
        function init(_, subcb) {
            mb = new lib.MessageBus({ 'log': LOG, 'peers': peers });
            mb.on('ready', subcb);
        },
        function partition(_, subcb) {
            mb.partition('0');
            var gMessageId;
            function onResponse(err, messageId, from) {
                t.ok(err);
                t.equal('RequestFailedError', err.name);
                t.equal(0, Object.keys(mb.messages).length);
                t.equal(messageId, gMessageId);
                t.equal(from, '1');
                return (subcb());
            }
            gMessageId = mb.send('0', '1', { 'operation': 'fake' }, onResponse);
        },
        function unpartition(_, subcb) {
            mb.unpartition('0');
            var responseCalled = false;
            var gMessageId;
            function onResponse(err, messageId, from) {
                responseCalled = true;
                t.ok(err === undefined);
                t.equal(messageId, gMessageId);
                t.equal(from, '1');
            }
            gMessageId =
                mb.send('0', '1', { 'operation': 'appendEntries' }, onResponse);
            setImmediate(function () {
                t.equal(1, Object.keys(mb.messages).length);
                mb.tick(function () {
                    t.ok(responseCalled);
                    subcb();
                });
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


test('partition while request to server', function (t) {
    var mb;
    var peers = getTopology(2);
    var funcs = [
        function init(_, subcb) {
            mb = new lib.MessageBus({ 'log': LOG, 'peers': peers });
            mb.on('ready', subcb);
        },
        function partitionClient(_, subcb) {
            var responseCalled = false;
            var gMessageId;
            function onResponse(err, messageId, from) {
                t.ok(err);
                t.equal('RequestFailedError', err.name);
                responseCalled = true;
                t.equal(messageId, gMessageId);
                t.equal(from, '1');
            }
            gMessageId =
                mb.send('0', '1', { 'operation': 'appendEntries' }, onResponse);
            setImmediate(function () {
                t.equal(1, Object.keys(mb.messages).length);
                mb.partition('0');
                mb.tick(function () {
                    t.ok(responseCalled);
                    t.equal(0, Object.keys(mb.messages).length);
                    subcb();
                });
            });
        },
        function partitionServer(_, subcb) {
            mb.unpartition('0');
            mb.unpartition('1');
            var responseCalled = false;
            var gMessageId;
            function onResponse(err, messageId, from) {
                t.ok(err);
                t.equal('RequestFailedError', err.name);
                responseCalled = true;
                t.equal(messageId, gMessageId);
                t.equal(from, '1');
            }
            gMessageId =
                mb.send('0', '1', { 'operation': 'appendEntries' }, onResponse);
            setImmediate(function () {
                t.equal(1, Object.keys(mb.messages).length);
                mb.partition('1');
                mb.tick(function () {
                    t.ok(responseCalled);
                    t.equal(0, Object.keys(mb.messages).length);
                    subcb();
                });
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


test('partition while response', function (t) {
    var mb;
    var peers = getTopology(2);
    var funcs = [
        function init(_, subcb) {
            mb = new lib.MessageBus({ 'log': LOG, 'peers': peers });
            mb.on('ready', function () {
                mb.peers['1'].wasCalled = false;
                mb.peers['1'].appendEntries = function (req, cb) {
                    mb.peers['1'].wasCalled = true;
                    mb.partition('0');
                    cb();
                };
                subcb();
            });
        },
        function partitionClient(_, subcb) {
            var responseCalled = false;
            var gMessageId;
            function onResponse(err, messageId, from) {
                t.ok(err);
                t.equal('RequestFailedError', err.name);
                responseCalled = true;
                t.equal(messageId, gMessageId);
                t.equal(from, '1');
            }
            gMessageId =
                mb.send('0', '1', { 'operation': 'appendEntries' }, onResponse);
            setImmediate(function () {
                t.equal(1, Object.keys(mb.messages).length);
                mb.tick(function () {
                    t.ok(responseCalled);
                    t.equal(0, Object.keys(mb.messages).length);
                    t.ok(mb.peers['1'].wasCalled);
                    subcb();
                });
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
