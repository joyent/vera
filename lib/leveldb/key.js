/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bignum = require('bignum');
var error = require('../error');



///--- Functions

/**
 * LevelDb recommends that the same key type be used for everything in a db.
 * I'm willing to take that advice.  So in the db, all keys are binary buffers
 * and all values are json objects.  We do need to namespace, so we're reserving
 * the first two bytes for a 'type' field.  Here are the current "registered"
 * types:
 *
 * 0x0000 - String properties
 * 0x0001 - Internal String properties
 * 0x0002 - Log entries
 * 0x0003 - State machine entries
 *
 * The reason we split up the properties sections into 2 is so that that the log
 * and the state machine have places to keep "internal" properties.  This makes
 * snapshots convenient - start streaming at 0x0001, and the log and state
 * machine have everything they need.
 */
function property(s) {
    var b = new Buffer(2 + Buffer.byteLength(s, 'utf8'));
    b[0] = 0x00;
    b[1] = 0x00;
    b.write(s, 2);
    return (b);
}


function internalProperty(s) {
    var b = new Buffer(2 + Buffer.byteLength(s, 'utf8'));
    b[0] = 0x00;
    b[1] = 0x01;
    b.write(s, 2);
    return (b);
}


function propertyDecode(b) {
    return (b.toString('utf-8', 2));
}


function log(i) {
    //Since the next int can't be represented in js, we throw when it is ===.
    // This is really just a safety net since the log would have to grow by
    // 1,000,000 per second for 285 years before this int is reached.
    if (i >= 9007199254740992) {
        throw (new error.InternalError(
            'log index at maximum javascript integer'));
    }
    //We allow 8 bytes for the key because... well... it seems weird to only
    // do 7 bytes, which is what's needed for 9007199254740992.
    var bn = bignum(i).toBuffer({ 'size': 10 });
    bn[0] = 0x00;
    bn[1] = 0x02;
    return (bn);
}


function logDecode(b) {
    return (bignum.fromBuffer(b.slice(2)).toNumber());
}


///--- Exports

module.exports = {
    'internalProperty': internalProperty,
    'property': property,
    'propertyDecode': propertyDecode,
    'log': log,
    'logDecode': logDecode
};
