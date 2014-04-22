// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var events = require('events');
var raftInit = require('./raft_init');
var restify = require('restify');
var util = require('util');
var vasync = require('vasync');
var watershed = require('watershed');


/**
 * The server binds a raft instance to the Vera interface.
 */

function Server(opts) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');
    assert.number(opts.port, 'opts.port');
    assert.string(opts.name, 'opts.name');
    assert.string(opts.version, 'opts.version');
    assert.object(opts.raft, 'opts.raft');

    var self = this;
    // Make all opts properties of self.
    Object.keys(opts).forEach(function (k) {
        self[k] = opts[k];
    });

    init.call(self);
}

util.inherits(Server, events.EventEmitter);
module.exports = Server;



//--- Helpers

function createServer(cb) {
    var self = this;
    var log = self.log;

    var server = restify.createServer({
        'name': self.name,
        'version': self.version,
        'log': self.log,
        'handleUpgrades': false
    });
    server.pre(restify.pre.userAgentConnection());

    //connect

    //data
    server.get(/^\/data(\/.*)/, function (req, res, next) {
        var list = req.params[0];
        log.info(list);
        res.send(200);
        return (next());
    });

    //raft
    server.get('/raft', function (req, res, next) {
        res.send(200);
        return (next());
    });

    //ping
    server.get('/ping', function (req, res, next) {
        //TODO: What should be "healthy"?
        res.send(200);
        return (next());
    });

    server.on('after', restify.auditLogger({
        'log': log
    }));

    server.listen(self.port, function () {
        log.info({
            'port': self.port
        }, 'server listening');
        return (cb(null, server));
    });
}


function init() {
    var self = this;
    var log = self.log;

    vasync.pipeline({
        funcs: [
            function initRaft(_, subcb) {
                log.info('initing raft');
                self.raft.log = log;
                raftInit.from(self.raft, function (err, raft) {
                    if (err) {
                        return (subcb(err));
                    }
                    //Replace the raft opts with the raft instance
                    self.raftOpts = self.raft;
                    self.raft = raft;
                    log.info('done initing raft');
                    return (subcb());
                });
            },
            function initServer(_, subcb) {
                log.info('initing server');
                createServer.call(self, function (err, server) {
                    if (err) {
                        return (subcb(err));
                    }
                    self.server = server;
                    log.info('done initing server');
                    return (subcb());
                });
            }
        ]
    }, function (err) {
        if (err) {
            self.emit('error', err);
            return;
        }
        self.emit('ready');
    });
}


//--- API

Server.prototype.close = function close() {
    var self = this;
    var log = self.log;

    log.info('shutting down server');
    // TODO
};
