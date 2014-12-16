/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var events = require('events');
var error = require('../error');
var key = require('./key');
var sprintf = require('extsprintf').sprintf;
var util = require('util');



/**
 * A state machine that keeps data under paths.
 *
 * In some sense, this follows the Vera interface, but I've tried to keep it
 * as independant as possible.
 */

///--- Globals

var COMMIT_INDEX_KEY = 'stateMachineCommitIndex';



///--- Functions

function StateMachine(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.db, 'opts.db');

    var self = this;
    self.log = opts.log;
    self.db = opts.db;

    self.commitIndex = 0;

    verifyDb.call(self);
}

util.inherits(StateMachine, events.EventEmitter);
module.exports = StateMachine;


///--- Helpers

function verifyDb() {
    var self = this;
    var props = {};

    function ready() {
        self.ready = true;
        self.emit('ready');
    }

    function persistProps() {
        var batch = [];
        Object.keys(props).forEach(function (k) {
            batch.push({ 'type': 'put', 'key': key.internalProperty(k),
                         'value': props[k] });
        });
        if (batch.length > 0) {
            self.db.batch(batch, function (err) {
                if (err) {
                    return (self.emit('error', err));
                }
                ready();
            });
        } else {
            ready();
        }
    }

    function loadCommitIndex() {
        self.db.get(key.internalProperty(COMMIT_INDEX_KEY),
                    function (err, val) {
                        if (err && err.name === 'NotFoundError') {
                            props[COMMIT_INDEX_KEY] = 0;
                        } else if (err) {
                            return (self.emit('error', err));
                        } else {
                            self.commitIndex = val;
                        }
                        persistProps();
                    });
    }

    loadCommitIndex();
}


function executeEntry(entry, cb) {
//    var self = this;
    //TODO: Actually execute...
    setImmediate(cb.bind(null, new error.InternalError('not implemented')));

//    var batch = [];
//    if (nCommitIndex !== undefined && nCommitIndex !== null) {
//        batch.push({ 'type': 'put',
//                     'key': key.internalProperty(COMMIT_INDEX_KEY),
//                     'value': nCommitIndex });
//    }
//    self.db.batch(batch, function (err) {
//        if (!err) {
//            self.commitIndex = nCommitIndex;
//        }
//        return (cb(err));
//    });

//TODO: Still need to update the commit index.
//            if (entry.command === 'noop') {

}



///--- API

//TODO: Is there a better way to do this?  Yuck!
StateMachine.prototype.from = function (db, cb) {
    assert.object(db, 'db');
    assert.func(cb, 'cb');

    var self = this;
    var sm = new StateMachine({
        'log': self.log,
        'db': db
    });
    sm.on('ready', function () {
        return (cb(null, sm));
    });
};


StateMachine.prototype.execute = function (entries, cb) {
    assert.object(entries, 'entries');
    assert.func(cb, 'cb');

    var self = this;
    if (!self.ready) {
        return (setImmediate(cb.bind(
            null, new error.InternalError('I wasn\'t ready yet.'))));
    }
    var nCommitIndex = self.commitIndex;

    entries.on('readable', function () {
        var entry;

        function next() {
            if (null === (entry = entries.read())) {
                return;
            }

            if (entry.index !== (nCommitIndex + 1)) {
                //A little heavy-handed here...
                entries.removeAllListeners();
                return (cb(new error.InternalError(sprintf(
                    'out of order commit, commit at %d, tried to apply entry ' +
                        'with index %d', self.commitIndex, entry.index))));
            }

            executeEntry.call(self, entry, function (err) {
                if (err) {
                    //TODO: We shouldn't be returning here... we should gather
                    // up the errors to return at the end... right?
                    return (cb(err));
                }
                ++nCommitIndex;
                next();
            });
        }

        next();
    });

    entries.on('end', function () {
        return (cb());
    });
};



///--- For Debugging

StateMachine.prototype.dump = function () {
    var self = this;
    self.db.createReadStream().on('data', console.log);
};
