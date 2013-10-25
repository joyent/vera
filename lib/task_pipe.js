// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');



/**
 * A pipeline of tasks is composed of many of these, each stage having a next.
 * Each stage in the pipeline can decide how many tasks it can consume at once.
 *
 * 'opts.func' should have a signature like this:
 *
 * <code>
 * function (opts, cb) {
 *    //code
 *    cb(err, res);
 * }
 * </code>
 *
 * If one stage of the pipeline encounters an error, the pipeline for that task
 * will exit.
 *
 * If 'opts.next' is specified, the request will be appended to the next in
 * line, with the res (from the previous step) used as the opts.  If there was
 * no result, opts will be passed instead.  Some patterns you might follow,
 * depending on your use case:
 *
 * <code>
 * //Just pass the opts along
 * function (opts, cb) {
 *    //code
 *    cb(err, opts);
 * }
 *
 * //Pass both the res and the opts along
 * function (opts, cb) {
 *    //code
 *    cb(err, { res: res, opts: opts });
 * }
 * </code>
 *
 *
 * If 'opts.choose' is specified, the list of requests are handed over to the
 * function, and the caller should return the number of items that should be
 * consumed from the front of the list.  Signature should be something like:
 *
 * <code>
 * function (requests) {
 *    //decide how many
 *    return (howMany);
 * }
 * <code>
 *
 * We could make the chooser actually consume from the list and return the set,
 * but this way there's a hint of "don't actually touch the list, please".
 */

///--- Functions

function TaskPipe(opts) {
    assert.object(opts, 'opts');
    assert.string(opts.name, 'opts.name');
    assert.func(opts.func, 'opts.func');
    assert.optionalFunc(opts.choose, 'opts.choose');
    assert.optionalObject(opts.next, 'opts.next');
    if (opts.next !== undefined) {
        assert.func(opts.next.append, 'next.append');
    }

    var self = this;
    self.choose = opts.choose || function () { return 1; };
    self.name = opts.name;
    self.func = opts.func;
    self.next = opts.next;

    self.todo = [];
    self.doing = undefined;
    self.inProgress = false;
}

module.exports = TaskPipe;



///--- Helpers

function consumeSome() {
    var self = this;
    if (self.inProgress || self.todo.length === 0) {
        return;
    }

    self.inProgress = true;
    self.doing = self.todo.splice(0, self.choose(self.todo));
    var d = self.doing.map(function (e) { return e.opts; });
    self.func(d, function (err, res) {
        self.doing.forEach(function (t) {
            var ret = (res === undefined || res === null) ? t.opts : res;
            if (err) {
                return (t.cb(err, ret));
            }
            if (self.next) {
                self.next.append(ret, t.cb);
            } else {
                t.cb(null, ret);
            }
        });
        self.doing = undefined;
        self.inProgress = false;
        consumeSome.call(self);
    });
}



///--- API

TaskPipe.prototype.append = function (opts, cb) {
    assert.ok(opts !== undefined, 'opts is undefined');
    assert.func(cb, 'cb');

    var self = this;
    self.todo.push({ 'opts': opts, 'cb': cb });
    //Allows some tasks to pile up during this tick.
    process.nextTick(consumeSome.bind(self));
};
