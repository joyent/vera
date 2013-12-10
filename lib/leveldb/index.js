// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
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
 * the first two bytes for a 'type' field.  So here are the current "registered"
 * types:
 *
 * 0x0000 - String properties
 * 0x0001 - Log entries
 */
function propertyKey(s) {
    var b = new Buffer(2 + Buffer.byteLength(s, 'utf8'));
    b.fill(0, 0, 2);
    b.write(s, 2);
    return (b);
}


//TODO: See the justification, but this is *woefully* insufficient and must be
// changed.  Ideally, I'd like to stick with a regular javascript number (which
// can go up to 2^53 and should be sufficient).
function logKey(i) {
    var b = new Buffer([ 0, 1, 0, 0, 0, 0 ]);
    b.writeUInt32BE(i, 2);
    return (b);
}



///--- Exports

module.exports = {
    'createOrOpen': createOrOpen,
    'errToObj': errToObj,
    'propertyKey': propertyKey,
    'logKey': logKey
};
