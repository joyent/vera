#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var bunyan = require('bunyan');
var dashdash = require('dashdash');
var dbHelpers = require('../lib/leveldb/db_helpers');
var path = require('path');



///--- Globals

var LOG = bunyan.createLogger({
    'level': (process.env.LOG_LEVEL || 'fatal'),
    'name': 'dumpdb',
    'stream': process.stdout
});

var OPTIONS = [
    {
        'names': ['help', 'h'],
        'type': 'bool',
        'help': 'Print this help and exit.'
    },
    {
        'names': ['location', 'l'],
        'type': 'string',
        'help': 'Location of the db.'
    }
];



///--- Main

var dd = dashdash.createParser({ options: OPTIONS });
try {
    var _opts = dd.parse(process.argv);
} catch (e) {
    console.error('dumpdb: error: %s', e.message);
    process.exit(1);
}

function help(msg) {
    var help = dd.help({ includeEnv: true }).trimRight();
    if (msg) {
        console.log('dumpdb: ' + msg);
    }
    console.log('WARNING: Do not run while Vera is running.\n'
                + 'usage: ' + path.basename(process.argv[1]) + ' [OPTIONS]\n'
                + 'options:\n'
                + help);
    process.exit(2);
}

if (_opts.help) {
    help();
}

if (!_opts.location) {
    help('location is a required parameter');
}

_opts.log = LOG;

dbHelpers.open(_opts, function (err, db) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    dbHelpers.dumpDbToConsole(db, function () {
        db.close(function (err) {
            if (err) {
                console.error(err);
                process.exit(1);
            }
            //Exit should exit happily here.
        });
    });
});

process.on('uncaughtException', function (err) {
    console.log('Caught exception: ' + err);
});
