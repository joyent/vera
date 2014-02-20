// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var memlib = require('../../lib/memory');
var memraft = require('../memory');
var vasync = require('vasync');


///--- Helpers

function popString(s) {
    var i = s.indexOf(' ');
    if (i === -1) {
        return ([s, '']);
    }
    return ([
        s.substring(0, i),
        s.substring(i + 1)
    ]);
}


function find(o, s) {
    var parts = s.split('.');
    parts.forEach(function (p) {
        if (p === '' || o === undefined) {
            return;
        }
        o = o[p];
    });
    return (o);
}


function blank(s) {
    return (s === undefined || s === '');
}



//--- Functions

function executeFile(_, filename, cb) {
    fs.readFile(filename, { 'encoding': 'utf8' }, function (err, data) {
        if (err) {
            return (cb(err));
        }
        var cmds = data.split('\n');
        execute(_, cmds, cb);
    });
}


function execute(_, cmds, cb) {
    var i = 0;
    function nextCommand() {
        var command = cmds[i];
        if (command === undefined) {
            return (cb());
        }
        if (command === '' || command.indexOf('#') === 0) {
            ++i;
            return (nextCommand());
        }
        var parts = popString(command);
        var op = parts[0];
        if (!OPS[op]) {
            var error = new Error(op + ' is an unknown command');
            error.line = i + 1;
            error.text = command;
            return (cb(error));
        }
        if (_.batch) {
            _.console('> ' + command);
        }
        OPS[op](_, parts[1], function (err) {
            if (err) {
                err.line = i + 1;
                err.text = command;
                return (cb(err));
            }
            ++i;
            nextCommand();
        });
    }
    nextCommand();
}


function init(_, cb) {
    assert.object(_);
    assert.object(_.log);

    _.nextRaft = 0;
    _.raftIds = [];
    _.messageBus = new memlib.MessageBus({ 'log': _.log });
    _.messageBus.on('ready', cb);
    _.console = function () {
        if (!_.silent) {
            var args = [];
            for (var i = 0; i < arguments.length; ++i) {
                args.push(arguments[i]);
            }
            console.log.apply(null, args);
        }
    };
}


module.exports = {
    'init': init,
    'executeFile': executeFile,
    'execute': execute
};


///--- Operations

var OPS = {
    // _ -> (cleared)
    'reset': function (_, cmd, cb) {
        Object.keys(_).forEach(function (k) {
            if (k !== 'log') {
                delete _[k];
            }
        });
        init(_, cb);
    },

    // cluster <# nodes> [electLeader] ->
    //   _.cluster
    //   _.raft-[0..<# nodes - 1>]
    'cluster': function cluster(_, cmd, cb) {
        assert.object(_.messageBus, '_.messageBus');

        var parts = cmd.split(' ');
        var size = parseInt(parts[0], 10) || 3;
        var electLeader = (parts[1] === 'electLeader');
        var opts = {
            'log': _.log,
            'size': size,
            'electLeader': electLeader,
            'messageBus': _.messageBus,
            'idOffset': _.nextRaft
        };
        memraft.cluster(opts, function (err, c) {
            if (err) {
                return (cb(err));
            }
            _.cluster = c;
            Object.keys(_.cluster.peers).forEach(function (p) {
                var r = _.cluster.peers[p];
                _[r.id] = r;
                _.raftIds.push(r.id);
            });
            _.nextRaft = _.nextRaft + size;
            _.console(Object.keys(_.cluster.peers));
            return (cb());
        });
    },

    // raft
    'raft': function raft(_, cmd, cb) {
        assert.object(_.log, '_.log');
        assert.object(_.messageBus, '_.messageBus');

        var id = 'raft-' + _.nextRaft;
        ++_.nextRaft;
        var opts = {
            'log': _.log,
            'id': id,
            'messageBus': _.messageBus
        };
        memraft.raft(opts, function (err, r) {
            if (err) {
                return (cb(err));
            }
            _.raftIds.push(id);
            _[id] = r;
            _.console(id);
            cb();
        });

    },

    // print -> (dumps to stdout)
    'print': function print(_, cmd, cb) {
        var s = '';
        s += '# Rafts:\n';
        _.raftIds.forEach(function (id) {
            s += memraft.raftSummary(_[id]);
        });
        s += '# Messages:\n';
        s += memraft.messageBusSummary(_.messageBus);
        process.stdout.write(s);
        cb();
    },

    // tick <cluster || messageBus || rafts || raft-#> [clause]
    //  [clause] can be:
    //    <# of ticks>
    //    until <assert command> (see assert)
    // default is tick cluster 1
    'tick': function tick(_, cmd, cb) {
        var parts = popString(cmd);
        var what = parts[0] || 'cluster';
        var finishCondition;
        var error;
        var ticks;
        //Tick for some number
        if (blank(parts[1]) || !isNaN(parseInt(parts[1], 10))) {
            ticks = blank(parts[1]) ? 1 : parseInt(parts[1], 10);
            finishCondition = function () {
                return (ticks-- === 0);
            };
        //Tick until some assert condition
        } else if (parts[1].indexOf('until ') === 0) {
            ticks = 100;
            var lastAssertError;
            var assertCommand = parts[1].replace('until ', '');
            finishCondition = function () {
                if (ticks-- === 0) {
                    //We pass the error up in case it's a problem with their
                    // assert function.
                    error = lastAssertError;
                    return (true);
                }

                //This is synchronous
                var metCondition = true;
                //This is horribly inefficent.  But this is just a repl...
                OPS['assert'](_, assertCommand, function (err) {
                    if (err) {
                        lastAssertError = err;
                    }
                    metCondition = (err === undefined);
                });
                return (metCondition);
            };
        } else {
            return (cb(new Error('invalid tick clause: ' + parts[1])));
        }
        function tickNext() {
            function tryNext(err) {
                if (err) {
                    return (cb(err));
                }
                tickNext();
            }

            if (finishCondition()) {
                return (cb(error));
            }

            switch (what) {
            case 'messageBus':
                if (!_.messageBus) {
                    return (cb(new Error('messageBus isn\'t defined')));
                }
                _.messageBus.tick(tryNext);
                break;
            case 'cluster':
                if (!_.cluster) {
                    return (cb(new Error('cluster isn\'t defined')));
                }
                _.cluster.tick(tryNext);
                break;
            case 'rafts':
                _.raftIds.forEach(function (id) {
                    _[id].tick();
                });
                tryNext();
                break;
            default:
                if (_.raftIds.indexOf(what) === -1) {
                    return (cb(new Error('cannot tick unknown raft ' + what)));
                }
                _[what].tick();
                tryNext();
                break;
            }
        }
        tickNext();
    },

    // ls <object path>
    'ls': function ls(_, cmd, cb) {
        var o = find(_, cmd);
        if (o === undefined || (typeof (o) !== 'object')) {
            _.console('value: ' + o);
        } else {
            _.console(Object.keys(o).join('\n'));
        }
        cb();
    },

    // get <object path>
    'get': function get(_, cmd, cb) {
        //Always print to the console here.
        console.log(find(_, cmd));
        cb();
    },

    // set <object path> <json>
    'set': function set(_, cmd, cb) {
        var parts = popString(cmd);
        var path = parts[0];
        var json = parts[1];
        var newo;
        try {
            newo = JSON.parse(json);
        } catch (e) {
            return (cb(new Error('json parse failed for ' + json + ': ' +
                                 e.toString())));
        }

        //Pop off the last .x
        var spath = '';
        var name = path;
        var i = path.lastIndexOf('.');
        if (i !== -1) {
            spath = path.substring(0, i);
            name = path.substring(i + 1);
        }

        var o = find(_, spath);
        o[name] = newo;
        cb();
    },

    // assert <operation> <object path> <expected>
    // operation can be anything that node-assert-plus provides.
    // expected can be any json object or the string 'undefined'
    'assert': function assertIt(_, cmd, cb) {
        var parts = popString(cmd);
        var op = parts[0];
        parts = popString(parts[1]);
        var path = parts[0];
        var json = parts[1];
        var expected;

        if (blank(op) || blank(path)) {
            return (cb(new Error(
                'assert requires operation and path')));
        }

        //Explicitly look for the 'undefined' string.
        if (json !== 'undefined' && !blank(json)) {
            try {
                expected = JSON.parse(json);
            } catch (e) {
                return (cb(new Error('json parse failed for ' + json + ': ' +
                                     e.toString())));
            }
        }

        if ((typeof (assert[op])) !== 'function') {
            return (cb(new Error(op + ' is not an assert-plus function')));
        }
        var err;
        try {
            if (expected !== undefined) {
                assert[op](expected, find(_, path));
            } else {
                assert[op](find(_, path));
            }
        } catch (e) {
            err = e;
        }
        cb(err);
    },

    // tp --> Same as "tick", then "print"
    'tp': function tp(_, cmd, cb) {
        OPS.tick(_, cmd, function (err) {
            if (err) {
                return (cb(err));
            }
            OPS.print(_, '', cb);
        });
    },

    // try <and other command> --> _.lastError
    'try': function tryIt(_, cmd, cb) {
        var parts = popString(cmd);
        var op = parts[0];
        var command = parts[1];
        if (OPS[op] === undefined) {
            return (cb(new Error(op + ' is an unknown command')));
        }
        _.lastError = undefined;
        OPS[op](_, command, function (err) {
            if (err) {
                _.lastError = err;
            }
            cb();
        });
    },

    // request <to> <json request> -> _.lastResponse
    //   to can be:
    //     raft-# - Send to a specific raft instance
    //     leader - Send to whoever is the leader at the time
    //
    // The _.lastResponse is only set after the cluster has been ticked.  This
    // returns immediately after the request has been enqueued.
    'request': function request(_, cmd, cb) {
        assert.object(_, '_');

        var parts = popString(cmd);
        var to = parts[0];
        cmd = parts[1];

        if (blank(to) || blank(cmd)) {
            return (cb(new Error('to and json request are required')));
        }

        //Locate raft...
        var toRaft;
        if (to === 'leader') {
            assert.object(_.cluster, '_.cluster');

            Object.keys(_.cluster.peers).forEach(function (id) {
                if (_.cluster.peers[id].state === 'leader') {
                    toRaft = _.cluster.peers[id];
                }
            });
            if (toRaft === undefined) {
                return (cb(new Error('no leader elected')));
            }
        } else {
            toRaft = _.messageBus.peers[to];
            if (toRaft === undefined) {
                return (cb(new Error('no leader elected')));
            }
        }

        //Make the request
        var command;
        try {
            command = JSON.parse(cmd);
        } catch (e) {
            return (cb(new Error('json parse failed for ' + cmd + ': ' +
                                 e.toString())));
        }

        //Wrap if there isn't a command...
        if (!command.command) {
            command = {
                'command': command
            };
        }

        var calledBack = false;
        function onResponse(err, response) {
            if (!calledBack) {
                _.console(response);
                _.lastResponse = response;
                _.lastError = err;
                //In case this was a reconfigure...
                _.cluster.refreshPeers();
                calledBack = true;
            }
        }

        delete _.lastResponse;
        delete _.lastError;
        toRaft.clientRequest(command, onResponse);
        return (cb());
    },

    // partition <raft-#>
    'partition': function partition(_, cmd, cb) {
        if (_.messageBus.peers[cmd] === undefined) {
            return (cb(new Error('instance unknwn: ' + cmd)));
        }
        _.messageBus.partition(cmd);
        cb();
    },

    // unpartitions <raft-#>
    'unpartition': function unpartition(_, cmd, cb) {
        if (_.messageBus.peers[cmd] === undefined) {
            return (cb(new Error('instance unknwn: ' + cmd)));
        }
        _.messageBus.unpartition(cmd);
        cb();
    }
};
