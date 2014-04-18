#!/usr/bin/env node
// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var dashdash = require('dashdash');
var MemRaft = require('../test/memory');
var path = require('path');
var sprintf = require('extsprintf').sprintf;

var LOG = bunyan.createLogger({
    'level': (process.env.LOG_LEVEL || 'fatal'),
    'name': 'random-memory-raft',
    'stream': process.stdout
});

var OPTIONS = [
    {
        'names': ['help', 'h'],
        'type': 'bool',
        'help': 'Print this help and exit.'
    },
    {
        'names': ['size', 's'],
        'type': 'positiveInteger',
        'help': 'The size (in nodes) of the raft cluster.',
        'helpArg': 'SIZE',
        'default': 3
    }
];



///--- Main

var dd = dashdash.createParser({ options: OPTIONS });
try {
    var _opts = dd.parse(process.argv);
} catch (e) {
    console.error('random_raft: error: %s', e.message);
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

process.stdin.resume();
process.stdin.setEncoding('utf8');

MemRaft.cluster(_opts, function (err, cluster) {
    var count = 0;
    function next() {
        console.log(sprintf('%d:\n%s', count, cluster.toString()));

        ++count;
        if (count % 5 === 0) {
            process.stdin.once('data', function (chunk) {
                chunk = chunk.replace('\n', '');
                if (chunk === 'q') {
                    process.exit();
                } else {
                    next();
                }
            });
            return;
        }

        cluster.tick(next);
    }
    next();
});
