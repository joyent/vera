// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var util = require('util');

var ERRORS = {
    'InvalidRaftRequest': null,
    'InvalidIndex': null,
    'InvalidPeer': null,
    'InvalidTerm': null,
    'Internal': null,
    'NotLeader': null,
    'TermMismatch': null
};

module.exports = {};

Object.keys(ERRORS).forEach(function (e) {
    e += 'Error';
    module.exports[e] = function (msg) {
        Error.captureStackTrace(this, this.constructor || this);
        this.message = msg || 'Error';
    };
    util.inherits(module.exports[e], Error);
    module.exports[e].prototype.name = e;
});
