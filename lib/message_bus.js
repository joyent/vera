// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('./error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

/**
 * This message bus is paired with the server.  Vera is it's own client.
 */

///--- Functions

function MessageBus(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.optionalObject(opts.peers, 'opts.peers');

    var self = this;
    self.log = opts.log;
    self.peers = opts.peers || {};
    self.messageId = 0;

    setImmediate(function () {
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
    var messageId = self.messageId++;

    if (!self.ready) {
        setImmediate(cb.bind(
            null,
            new error.InternalError('I wasn\'t ready yet.'),
            messageId,
            to));
        return (messageId);
    }

    if (self.peers[to] === null || self.peers[to] === undefined) {
        setImmediate(cb.bind(
            null,
            new error.InvalidPeerError(
                sprintf('peer %s isn\'t known', to)),
            messageId,
            to));
        return (messageId);
    }

    //Add it to the message "queue"
    var m = {
        'from': from,
        'to': to,
        'message': message,
        'cb': cb
    };
    self.messages[messageId] = m;

    //TODO: This is the callback
    // m.cb(err, k, m.to, res);
    // delete self.messages[k];

    //TODO: Actually implement.
    setImmediate(m.cb.bind(null, new error.InternalError('not implemented'),
                           m.to, null));

    return (messageId);
};


MessageBus.prototype.register = function (peer) {
    assert.object(peer, 'peer');
    assert.string(peer.id, 'peer.id');

    var self = this;
    self.peers[peer.id] = peer;
};


//The real message bus would need to do more.
MessageBus.prototype.cancel = function (messageId) {
    var self = this;
    if (!self.ready) {
        throw new error.InternalError('I wasn\'t ready yet.');
    }
    if (self.messages[messageId] !== undefined) {
        var m = self.messages[messageId];
        m.cb(new error.RequestFailedError('request canceled'), messageId, m.to);
        delete self.messages[messageId];
    }
};



///--- For Debugging

MessageBus.prototype.dump = function () {
    var self = this;
    console.log({
        'peers': self.peers,
        'messages': self.messages
    });
};
