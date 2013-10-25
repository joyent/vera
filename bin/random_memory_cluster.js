// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var MemRaft = require('../test/raft');
var sprintf = require('extsprintf').sprintf;

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'random-memory-raft',
    stream: process.stdout
});

function parseOptions() {
    var option;
    var opts = {};
    opts.size = 3;
    var parser = new getopt.BasicParser('s:',
                                        process.argv);
    while ((option = parser.getopt()) !== undefined && !option.error) {
        switch (option.option) {
        case 's':
            opts.size = parseInt(option.optarg, 10);
            break;
        default:
            usage('Unknown option: ' + option.option);
            break;
        }
    }

    return (opts);
}

var _opts = parseOptions();
_opts.log = LOG;

process.stdin.resume();
process.stdin.setEncoding('utf8');

MemRaft.cluster(_opts, function (err, cluster) {
    var count = 0;
    function next() {
        console.log(sprintf('%d:\n%s\n', count, cluster.toString()));

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

        //Taken from the test/raft/index.js
        if (Object.keys(cluster.messageBus.messages).length > 0) {
            cluster.messageBus.tick(function () {
                return (process.nextTick(next));
            });
        } else {
            Object.keys(cluster.peers).forEach(function (p) {
                cluster.peers[p].tick();
            });
            return (process.nextTick(next));
        }
    }
    next();
});
