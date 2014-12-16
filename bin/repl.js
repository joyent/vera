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
var memraft = require('../test/repl/memory_raft');
var path = require('path');
var readline = require('readline');



///--- Globals

var LOG = bunyan.createLogger({
    'level': (process.env.LOG_LEVEL || 'fatal'),
    'name': 'memraft-repl',
    'stream': process.stdout
});

var OPTIONS = [
    {
        'names': ['help', 'h'],
        'type': 'bool',
        'help': 'Print this help and exit.'
    },
    {
        'names': ['filename', 'f'],
        'type': 'string',
        'help': 'File with repl commands, one per line.',
        'helpArg': 'FILE'
    },
    {
        'names': ['repl', 'r'],
        'type': 'bool',
        'help': 'Interactive repl on error or end of file commands.'
    }
];



///--- Main

var dd = dashdash.createParser({ options: OPTIONS });
try {
    var _opts = dd.parse(process.argv);
} catch (e) {
    console.error('repl: error: %s', e.message);
    process.exit(1);
}

if (_opts.help) {
    var help = dd.help({ includeEnv: true }).trimRight();
    console.log('usage: ' + path.basename(process.argv[1]) + ' [OPTIONS]\n'
                + 'options:\n'
                + help);
    process.exit(2);
}

_opts.log = LOG;
var props = { 'log': LOG };

function handleError(err) {
    if (!err) {
        return;
    }
    console.error(err);
    process.exit(1);
}

function repl(p) {
    p.batch = false;
    var rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    function doNext() {
        rl.question('> ', function (command) {
            if (command === 'quit' || command === 'q' ||
                command === 'exit') {
                rl.close();
            } else {
                memraft.execute(p, [ command ], function (err) {
                    if (err) {
                        console.error(err);
                    }
                    doNext();
                });
            }
        });
    }
    doNext();
}

memraft.init(props, function (err) {
    handleError(err);
    if (_opts.filename) {
        props.batch = true;
        memraft.executeFile(props, _opts.filename, function (err2) {
            if (err2) {
                console.error(err2);
            }
            if (_opts.repl) {
                repl(props);
            }
        });
    } else {
        repl(props);
    }
});

process.on('uncaughtException', function (err) {
    console.log('Caught exception: ' + err);
});
