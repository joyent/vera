// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../error');
var sprintf = require('extsprintf').sprintf;
var util = require('util');

/**
 * This is a testing message bus.  In the fullness of times, it should be able
 * to delay requests/responses between objects.
 *
 * The message bus will always call back, with an error if the peer is
 * unreachable.
 *
 * For the message bus to deliver messages, you must call tick();
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
    self.messages = {};
    self.partitioned = [];

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
    if (self.partitioned.indexOf(to) !== -1 ||
        self.partitioned.indexOf(from) !== -1) {
        setImmediate(cb.bind(
            null,
            new error.RequestFailedError('partitioned'),
            messageId,
            to));
        return (messageId);
    }

    //Add it to the message "queue"
    self.messages[messageId] = {
        'from': from,
        'to': to,
        'message': message,
        'cb': cb
    };

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


MessageBus.prototype.partition = function (id) {
    var self = this;
    if (!self.ready) {
        throw new error.InternalError('I wasn\'t ready yet.');
    }
    if (self.partitioned.indexOf(id) === -1) {
        self.partitioned.push(id);
    }
};


MessageBus.prototype.unpartition = function (id) {
    var self = this;
    if (!self.ready) {
        throw new error.InternalError('I wasn\'t ready yet.');
    }
    var index = self.partitioned.indexOf(id);
    if (index !== -1) {
        self.partitioned.splice(index);
    }
};


//Causes all messages to be delivered, and the next batch held.  In the real
// implementation, there shouldn't be any such thing as a 'tick'.   Messages are
// delivered when they arrive.
MessageBus.prototype.tick = function (cb) {
    assert.func(cb, 'cb');

    var self = this;
    var responses = 0;
    var thisBatch = Object.keys(self.messages);
    var total = thisBatch.length;
    if (total === 0) {
        return (setImmediate(cb));
    }
    //TODO: Partition in the middle/on the way back.
    thisBatch.forEach(function (k) {
        var m = self.messages[k];
        var p = self.peers[m.to];

        function onResponse(err, res) {
            if (self.messages[k] !== undefined) {
                //Partition on the way in...
                if (self.partitioned.indexOf(m.to) !== -1 ||
                    self.partitioned.indexOf(m.from) !== -1) {
                    //We overwrite whatever error or response.
                    err = new error.RequestFailedError('partitioned');
                    res = undefined;
                }

                //Can cause more messages to be enqueued.
                m.cb(err, k, m.to, res);
                delete self.messages[k];
            }
            ++responses;
            if (responses === total) {
                return (cb(null));
            }
        }

        //Partition on the way out...
        if (self.partitioned.indexOf(m.to) !== -1 ||
            self.partitioned.indexOf(m.from) !== -1) {
            return (onResponse(new error.RequestFailedError('partitioned')));
        }

        //Kinda wonky...?
        if (m.message.operation === 'appendEntries') {
            p.appendEntries(m.message, onResponse);
        } else if (m.message.operation === 'requestVote') {
            p.requestVote(m.message, onResponse);
        } else if (m.message.operation === 'installSnapshot') {
            p.installSnapshot(m.message, onResponse);
        } else {
            throw new Error(m.message.operation +
                            ' can\'t be sent, unknown operation.');
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
