// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../../lib/error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

/**
 * This is a testing message bus.  In the fullness of times, it should be able
 * to black-hole/delay requests/responses between objects.  For now it only
 * enables one raft object to message to another raft object.
 *
 * For the message bus to deliver messages, you must call tick();
 *
 * Since rafts can take multiple ticks to cb, is this even going to work?
 */

///--- Functions

function MessageBus(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.peers, 'opts.peers');

    var self = this;
    self.log = opts.log;
    self.peers = opts.peers;
    self.messageId = 0;
    self.messages = {};

    process.nextTick(function () {
        self.ready = true;
        self.emit('ready');
    });
}

util.inherits(MessageBus, events.EventEmitter);
module.exports = MessageBus;



///--- API

MessageBus.prototype.send = function (from, to, message, cb) {
    assert.string(from, 'from');
    assert.string(to, 'to');
    assert.object(message, 'message');
    assert.func(cb, 'cb');

    var self = this;
    if (self.peers[to] === null || self.peers[to] === undefined) {
        throw new error.InternalError(sprintf('peer %s isn\'t known', to));
    }
    if (!self.ready) {
        return (process.nextTick(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }

    var messageId = self.messageId++;

    //Add it to the message "queue"
    self.messages[messageId] = {
        'from': from,
        'to': to,
        'message': message,
        'cb': cb
    };

    return (messageId);
};


//The real message bus would need to do more.
MessageBus.prototype.cancel = function (messageId) {
    var self = this;
    if (!self.ready) {
        throw new error.InternalError('I wasn\'t ready yet.');
    }
    if (self.messages[messageId] !== undefined) {
        delete self.messages[messageId];
    }
};


MessageBus.prototype.tick = function () {
    var self = this;
    Object.keys(self.messages).forEach(function (k) {
        var m = self.messages[k];
        var p = self.peers[m.to];

        function onResponse(err, res) {
            //Block on incoming...
            //TODO: Should we enqueue on response, and deliver on next tick or
            // deliver immediately?
            if (self.messages[k] !== undefined) {
                m.cb(err, k, m.to, res);
                delete self.messages[k];
            }
        }

        //Kinda wonky...?
        if (m.message.operation === 'appendEntries') {
            p.appendEntries(m.message, onResponse);
        } else if (m.message.operation === 'requestVote') {
            p.requestVote(m.message, onResponse);
        }
    });
};



///--- For Debugging

MessageBus.prototype.dump = function () {
    var self = this;
    console.log({
        'peers': self.peers,
        'messages': self.messages
    });
};