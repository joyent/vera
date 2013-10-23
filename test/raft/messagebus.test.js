// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var MessageBus = require('./messagebus');
var vasync = require('vasync');



///--- Globals

var test = helper.test;
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
    process.nextTick(cb);
};

FakeRaft.prototype.appendEntries = function (req, cb) {
    var self = this;
    ++self.appendEntriesCalled;
    process.nextTick(cb);
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
        function (_, subcb) {
            mb = new MessageBus({ 'log': LOG, 'peers': peers });
            mb.on('ready', subcb);
        },
        function (_, subcb) {
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

            process.nextTick(function () {
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
