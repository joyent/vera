// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var memraft = require('./repl/memory_raft');
var fs = require('fs');
var path = require('path');
var test = require('nodeunit-plus').test;
var util = require('util');



/**
 * This sets up all the text files under repl_tests as nodeunit tests.  It does
 * this by recursively finding all the text files under 'repl_tests',
 * transforming the files names to
 */

///--- Globals

var LOG = bunyan.createLogger({
    level: (process.env.LOG_LEVEL || 'fatal'),
    name: 'memprops-test',
    stream: process.stdout
});



///--- Helpers

//Thank you stack overflow...
function findFiles(dirname) {
    var results = [];
    var list = fs.readdirSync(dirname);
    list.forEach(function (file) {
        file = dirname + '/' + file;
        var stat = fs.statSync(file);
        if (stat && stat.isDirectory()) {
            results = results.concat(findFiles(file));
        } else {
            results.push(file);
        }
    });
    return (results);
}


function makeTest(filename) {
    return (function (t) {
        var props = { 'log': LOG, 'silent': true };
        memraft.init(props, function (err) {
            if (err) {
                t.fail(err);
                return (t.done());
            }
            memraft.executeFile(props, filename, function (err2) {
                if (err2) {
                    console.error('' + err2);
                    console.error('At line: ' + err2.line);
                    console.error('Text: ' + err2.text);
                    console.error();
                    console.error('Get a repl at the error:');
                    console.error('./bin/memraft_repl.js -r -f ' +
                                  filename);
                    console.error();
                    t.fail(err);
                }
                t.done();
            });
        });
    });
}



///--- Tests are pulled from all files under repl_tests

var dir = path.resolve(__dirname, 'repl_tests');
var files = findFiles(dir);
var tests = [];
for (var i = 0; i < files.length; ++i) {
    var f = files[i];
    // /Users/george/projects/vera/test/repl_tests/test_name.txt ->
    // test name
    // /Users/george/projects/vera/test/repl_tests/foo/test_name.txt ->
    // foo: test name
    var testName = f.replace(dir + '/', '');
    testName = testName.replace('.txt', '');
    testName = testName.replace(/\//g, ': ');
    testName = testName.replace(/_/g, ' ');

    test(testName, makeTest(f));
}
