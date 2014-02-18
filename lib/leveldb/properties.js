// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var dbHelpers = require('./db_helpers');
var events = require('events');
var error = require('../../lib/error');
var leveldbkey = require('./key');
var lib = require('../leveldb');
var util = require('util');



/**
 * LevelDB (https://code.google.com/p/leveldb/) persisted properties.  Also
 * caches the properties in memory for fast gets.
 */

///--- Globals

var propertyKey = leveldbkey.property;
var errToObj = lib.errToObj;
var SYNC_OPTIONS = { 'sync': true };



///--- Functions

function LevelDbProperties(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.optionalString(opts.location, 'opts.location');
    assert.optionalObject(opts.db, 'opts.db');

    var self = this;
    self.log = opts.log;
    self.props = {};
    self.ready = false;
    openDb.call(self, opts);
}

util.inherits(LevelDbProperties, events.EventEmitter);
module.exports = LevelDbProperties;


///--- Helpers

/**
 * Does the actual initing of the leveldb log.  It will emit either ready or
 * error exactly once.
 */
function openDb(opts) {
    var self = this;
    var log = self.log;

    function setupSelf() {
        //Read all the properties we have persisted
        var rs = opts.db.createReadStream({
            'start': new Buffer([ 0, 0, 0 ]),
            //0xff isn't a valid utf-8 character.
            'end': new Buffer([ 0, 0, 255 ])
        });

        rs.on('readable', function () {
            var d;
            while (null !== (d = rs.read())) {
                var key = d.key.toString('utf-8', 2);
                var value = d.value;
                self.props[key] = value;
            }
        });

        rs.on('error', function (err) {
            rs.removeAllListeners();
            self.emit('error', err);
        });

        rs.on('end', function () {
            self.db = opts.db;
            self.ready = true;
            if (self.get('currentTerm') === undefined) {
                self.write({
                    'currentTerm': 0
                }, function (err2) {
                    if (err2) {
                        return (self.emit('error', err2));
                    }
                    self.emit('ready');
                });
            } else {
                self.emit('ready');
            }
        });
    }

    //If there's an open db passed in, then skip opening it ourselves.
    if (opts.db !== null && opts.db !== undefined) {
        return (setupSelf());
    }

    log.debug({ 'opts.location': opts.location },
              'opening leveldb log');
    dbHelpers.createOrOpen(opts, function (err, res) {
        if (err) {
            return (self.emit('error', err));
        }

        opts.db = res.db;
        setupSelf();
    });
}



///--- API

LevelDbProperties.prototype.write = function append(props, cb) {
    assert.object(props, 'props');
    var self = this;
    if (!self.ready) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    var db = self.db;

    var batch = [];
    Object.keys(props).forEach(function (k) {
        var v = props[k];
        batch.push({
            'type': 'put',
            'key': propertyKey(k),
            'value': v
        });
    });
    db.batch(batch, SYNC_OPTIONS, function (err) {
        //Also put in the properties cache.
        Object.keys(props).forEach(function (k) {
            self.props[k] = props[k];
        });
        cb(err);
    });
};


LevelDbProperties.prototype.get = function get(key) {
    assert.string(key, 'key');
    var self = this;
    if (!self.ready) {
        throw new error.InternalError('I wasn\'t ready yet.');
    }
    return (self.props[key]);
};


LevelDbProperties.prototype.delete = function (key, cb) {
    assert.string(key, 'key');
    var self = this;
    if (!self.ready) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    self.db.del(propertyKey(key), function (err) {
        if (err) {
            return (cb(err));
        }
        delete self.props[key];
        return (cb());
    });
};


LevelDbProperties.prototype.close = function close(cb) {
    cb = cb || function () {};
    var self = this;
    if (self.open === false) {
        return (setImmediate(cb));
    }
    if (self.db.isOpen()) {
        self.db.close(function () {
            self.open = false;
            return (cb());
        });
    }
};


//TODO: Only dump properties?
LevelDbProperties.prototype.dump = function dump(cb) {
    cb = cb || function () {};
    var self = this;
    var db = self.db;
    db.createReadStream()
        .on('data', function (d) {
            console.log(JSON.stringify(d, null, 0));
        }).on('close', cb);
    return;
};
