// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var levelup = require('level');
var leveldbkey = require('./key');



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
 * Dumps a leveldb to the console
 */
function dumpDbToConsole(db, cb) {
    var rs = db.createReadStream();
    var types = [
        { 'type': 'prp', 'keyDecode': leveldbkey.propertyDecode },
        { 'type': 'ipr', 'keyDecode': leveldbkey.propertyDecode },
        { 'type': 'log', 'keyDecode': leveldbkey.logDecode }
    ];
    rs.on('data', function (data) {
        var t = types[data.key[1]] || {
            'type': 'unknown',
            'keyDecode': function (x) { return (x); }
        };
        console.log(JSON.stringify({
            'type': t.type,
            'key': t.keyDecode(data.key),
            'value': data.value
        }, null, 0));
    });
    rs.on('end', function () {
        cb();
    });
}


///--- Exports

module.exports = {
    'createOrOpen': createOrOpen,
    'dumpDbToConsole': dumpDbToConsole
};
