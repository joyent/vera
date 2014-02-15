// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var memraftText = require('../test/memraft_text');
var path = require('path');
var readline = require('readline');

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'random-memory-raft',
    stream: process.stdout
});


///--- Opts parsing


function parseOptions() {
    var option;
    var opts = {};
    var parser = new getopt.BasicParser('f:r',
                                        process.argv);
    while ((option = parser.getopt()) !== undefined && !option.error) {
        switch (option.option) {
        case 'f':
            opts.filename = option.optarg;
            break;
        case 'r':
            opts.repl = true;
            break;
        default:
            usage('Unknown option: ' + option.option);
            break;
        }
    }

    return (opts);
}


function usage(msg) {
    if (msg) {
        console.error(msg);
    }
    var str  = 'usage: ' + path.basename(process.argv[1]);
    str += ' -f <filename>';
    str += ' -r';
    console.error(str);
    process.exit(1);
}



///--- Main

var _opts = parseOptions();
_opts.log = LOG;

function handleError(err) {
    if (!err) {
        return;
    }
    console.error(err);
    process.exit(1);
}

function repl(_) {
    _.batch = false;
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
                memraftText.execute(_, [ command ], function (err) {
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

var _ = { 'log': LOG };
memraftText.init(_, function (err) {
    handleError(err);
    if (_opts.filename) {
        _.batch = true;
        memraftText.executeFile(_, _opts.filename, function (err) {
            if (err) {
                console.error(err);
            }
            if (_opts.repl) {
                repl(_);
            }
        });
    } else {
        repl(_);
    }
});
