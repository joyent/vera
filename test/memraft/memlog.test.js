// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var bunyan = require('bunyan');
var helper = require('../helper.js');
var lib = require('../../lib');
var MemLog = require('./memlog');
var StateMachine = require('./statemachine');
var vasync = require('vasync');

// All the actual tests are here...
var commandLogTests = require('../share/command_log_tests.js');



///--- Globals

var before = helper.before;
var test = helper.test;
var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memlog-test',
    stream: process.stdout
});


///--- Setup/Teardown

before(function (cb) {
    var self = this;
    vasync.pipeline({
        funcs: [
            function initStateMachine(_, subcb) {
                self.stateMachine = new StateMachine({ 'log': LOG });
                self.stateMachine.on('ready', subcb);
            },
            function initMemLogHere(_, subcb) {
                self.clog = new MemLog({ 'log': LOG,
                                    'stateMachine': self.stateMachine });
                self.clog.on('ready', subcb);
            }
        ]
    }, function (err) {
        cb(err);
    });
});
