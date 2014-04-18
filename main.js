// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var fs = require('fs');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var dashdash = require('dashdash');
var dtrace = require('dtrace-provider');
var path = require('path');
var restify = require('restify');

var lib = require('./lib');



///--- Globals

var NAME = 'vera';
var LEVEL = process.env.LOG_LEVEL || 'info';
var LOG = bunyan.createLogger({
    'name': NAME,
    'streams': [ {
        'level': LEVEL,
        'stream': process.stdout
    } ],
    'serializers': restify.bunyan.serializers
});
var OPTIONS = [
    {
        'names': ['help', 'h'],
        'type': 'bool',
        'help': 'Print this help and exit.'
    },
    {
        'names': ['file', 'f'],
        'type': 'string',
        'help': 'Configuration file to use.',
        'helpArg': 'FILE'
    },
    {
        'names': ['port', 'p'],
        'type': 'positiveInteger',
        'help': 'Listen for requests on port.',
        'helpArg': 'PORT'
    },
    {
        'names': ['verbose', 'v'],
        'type': 'arrayOfBool',
        'help': 'Verbose output. Use multiple times for more verbose.'
    }
];

var VERSION = false;



///--- Internal Functions

function configure() {
    var cfg;
    var opts;
    var parser = new dashdash.Parser({options: OPTIONS});

    var dd = dashdash.createParser({ options: OPTIONS });
    try {
        opts = dd.parse(process.argv);
        assert.object(opts, 'options');
    } catch (e) {
        LOG.fatal(e, 'invalid options');
        process.exit(1);
    }

    function helpExit(msg) {
        var help = dd.help({ includeEnv: true }).trimRight();
        if (msg) {
            console.error('vera: ' + msg);
        }
        console.log('usage: ' + path.basename(process.argv[1]) + ' [OPTIONS]\n'
                    + 'options:\n'
                    + help);
        process.exit(2);
    }

    if (opts.help) {
        helpExit();
    }

    if (!opts.file) {
        helpExit('file (-f or --file) is a required parameter');
    }

    cfg = JSON.parse(readFile(opts.file));
    cfg.port = opts.port || cfg.port || 1919;

    cfg.name = NAME;
    cfg.version = version();

    if (opts.verbose) {
        opts.verbose.forEach(function () {
            LOG.level(Math.max(bunyan.TRACE, (LOG.level() - 10)));
        });
    }

    if (LOG.level() <= bunyan.DEBUG)
        LOG = LOG.child({src: true});

    LOG.debug(cfg, 'createServer: config loaded');

    cfg.log = LOG;
    return (cfg);
}


function readFile(file) {
    var data;

    try {
        data = fs.readFileSync(file, 'utf8');
    } catch (e) {
        console.error('Unable to read file %s: %s', file, e.message);
        process.exit(1);
    }

    return (data);
}


function version() {
    if (!VERSION) {
        var fname = __dirname + '/package.json';
        var pkg = fs.readFileSync(fname, 'utf8');
        VERSION = JSON.parse(pkg).version;
    }

    return (VERSION);
}



///--- Mainline

(function main() {
    var cfg = configure();

    var dtp = dtrace.createDTraceProvider('vera');
    var client_close = dtp.addProbe('client_close', 'json');
    var socket_timeout = dtp.addProbe('socket_timeout', 'json');
    client_close.dtp = dtp;
    socket_timeout.dtp = dtp;
    dtp.enable();

    cfg.dtrace_probes = {
        client_close: client_close,
        socket_timeout: socket_timeout
    };

    var server = new lib.Server(cfg);

    server.on('error', function (err) {
        LOG.fatal(err, 'server error');
        process.exit(1);
    });

    server.on('ready', function () {
        LOG.info('%s listening on port %d', NAME, server.port);
    });

    process.on('SIGHUP', process.exit.bind(process, 0));

})();
