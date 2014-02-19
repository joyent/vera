// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var fs = require('fs');
var memraft = require('../memory');
var MessageBus = require('../messagebus');
var test = require('nodeunit-plus').test;
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
            return (cb(error));
        }
        if (_.batch) {
            console.log('> ' + command);
        }
        OPS[op](_, parts[1], function (err) {
            if (err) {
                err.line = i + 1;
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
    _.messageBus = new MessageBus({ 'log': _.log });
    _.messageBus.on('ready', cb);
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

    // cluster <# nodes> ->
    //   _.cluster
    //   _.raft-[0..<# nodes - 1>]
    'cluster': function cluster(_, cmd, cb) {
        assert.object(_.messageBus, '_.messageBus');

        var size = parseInt(cmd, 10) || 3;
        var opts = {
            'log': _.log,
            'size': size,
            'electLeader': false,
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
            console.log(Object.keys(_.cluster.peers));
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
            console.log(id);
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

    // tick <cluster || messageBus || rafts || raft-#> <# of ticks>
    // default is tick messageBus 1
    'tick': function tick(_, cmd, cb) {
        var parts = cmd.split(' ');
        var what = parts[0] || 'cluster';
        var times = parts[1] === undefined ? 1 : parseInt(parts[1], 10);
        function tickNext() {
            function tryNext(err) {
                if (err) {
                    return (cb(err));
                }
                --times;
                tickNext();
            }

            if (times === 0) {
                return (cb());
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
            console.log('value: ' + o);
        } else {
            console.log(Object.keys(o).join('\n'));
        }
        cb();
    },

    // get <object path>
    'get': function get(_, cmd, cb) {
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
            return (cb(e));
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
    'assert': function assertIt(_, cmd, cb) {
        var parts = popString(cmd);
        var op = parts[0];
        parts = popString(parts[1]);
        var path = parts[0];
        var json = parts[1];
        var expected;

        function blank(s) {
            return (s === undefined || s === '');
        }

        if (blank(op) || blank(path) || blank(json)) {
            return (cb(new Error(
                'assert requires operation, path and expected')));
        }

        try {
            expected = JSON.parse(json);
        } catch (e) {
            return (cb(new Error('json parse failed for ' + json + ": " +
                                 e.toString())));
        }

        if ((typeof (assert[op])) !== 'function') {
            return (cb(new Error(op + ' is not an assert-plus function')));
        }
        var err;
        try {
            assert[op](expected, find(_, path));
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
    }
};
