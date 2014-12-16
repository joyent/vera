/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var util = require('util');

var ERRORS = {
    'InvalidRaftRequest': null,
    'InvalidIndex': null,
    'InvalidPeer': null,
    'InvalidTerm': null,
    'Internal': null,
    'NotBootstrapped': null,
    'NotLeader': null,
    'RequestFailed': null,
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
