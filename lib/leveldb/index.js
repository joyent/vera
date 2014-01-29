// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bignum = require('bignum');
var error = require('../error');
var levelup = require('level');


///--- Functions

/**
 * Creates or opens an existing leveldb.
 */
function createOrOpen(opts, cb) {
    assert.object(opts.log, 'opts.log');
    assert.string(opts.location, 'opts.location');
    var log = opts.log;
    var levelOpts = {
        'createIfMissing': false,
        'keyEncoding': 'binary',
        'valueEncoding': 'json'
    };
    levelup(opts.location, levelOpts, function (err, db) {
        log.debug({ 'location': opts.location, 'err': errToObj(err),
                    'hasError': err !== undefined && err !== null },
                  'result of trying to open existing db');
        //Unfortunately, the error code for any type of error is 'OpenError',
        // whether it is already open by the process or just doesn't exist.  So
        // we introspect the message (yuck!) to see if the OpenError is the
        // thing we want to ignore.
        if (err && (err.name !== 'OpenError' ||
                    err.message.indexOf('(create_if_missing is false)') ===
                    -1)) {
            log.fatal({ 'error': errToObj(err) }, 'Error opening leveldb');
            return (cb(err));
        }

        //Already exists.
        if (!err) {
            log.debug({ 'location': opts.location }, 'Opened leveldb log');
            return (cb(null, { 'created': false, 'db': db }));
        }

        //So there was an err because it doesn't exist.
        log.debug({ 'location': opts.location }, 'Creating new leveldb log');
        levelOpts.createIfMissing = true;
        levelup(opts.location, levelOpts, function (err2, dbc) {
            if (err2) {
                log.fatal({ 'error': errToObj(err2) },
                          'Error creating leveldb');
                return (cb(err2));
            }

            return (cb(null, { 'created': true, 'db': dbc }));
        });
    });
}


function errToObj(err) {
    if (!err) {
        return (undefined);
    }
    return ({
        'name': err.name === undefined ? '(unknown)' : err.name,
        'message': err.message === undefined ? '(none)' : err.message
    });
}


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
function propertyKey(s) {
    var b = new Buffer(2 + Buffer.byteLength(s, 'utf8'));
    b[0] = 0x00;
    b[1] = 0x00;
    b.write(s, 2);
    return (b);
}


function internalPropertyKey(s) {
    var b = new Buffer(2 + Buffer.byteLength(s, 'utf8'));
    b[0] = 0x00;
    b[1] = 0x01;
    b.write(s, 2);
    return (b);
}


function propertyKeyDecode(b) {
    return (b.toString('utf-8', 2));
}


function logKey(i) {
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


function logKeyDecode(b) {
    return (bignum.fromBuffer(b.slice(2)).toNumber());
}



///--- Exports

module.exports = {
    'createOrOpen': createOrOpen,
    'errToObj': errToObj,
    'internalPropertyKey': internalPropertyKey,
    'propertyKey': propertyKey,
    'propertyKeyDecode': propertyKeyDecode,
    'logKey': logKey,
    'logKeyDecode': logKeyDecode
};
