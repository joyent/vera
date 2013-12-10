// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var error = require('../../lib/error');
var util = require('util');



/**
 * Keeps a set of properties in memory, meant to look like the interface for the
 * same thing that persists to disk.
 */

///--- Functions

function MemProps(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.optionalObject(opts.props, 'opts.props');

    var self = this;
    self.log = opts.log;
    self.props = opts.props || {};
    self.ready = false;

    process.nextTick(function () {
        self.ready = true;
        self.emit('ready');
    });
}

util.inherits(MemProps, events.EventEmitter);
module.exports = MemProps;



///--- API

MemProps.prototype.write = function (props, cb) {
    assert.object(props, 'props');

    var self = this;
    if (!self.ready) {
        return (process.nextTick(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }

    process.nextTick(function () {
        Object.keys(props).forEach(function (k) {
            self.props[k] = props[k];
        });
        return (cb(null));
    });
};


MemProps.prototype.get = function (key) {
    assert.string(key, 'key');
    var self = this;
    if (!self.ready) {
        throw new error.InternalError('I wasn\'t ready yet.');
    }
    return (self.props[key]);
};


MemProps.prototype.delete = function (key, cb) {
    assert.string(key, 'key');
    var self = this;
    if (!self.ready) {
        return (process.nextTick(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    return (process.nextTick(function () {
        delete self.props[key];
        return (cb(null));
    }));
};


///--- For Debugging

MemProps.prototype.dump = function () {
    var self = this;
    console.log({
        'props': self.props
    });
};
