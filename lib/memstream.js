// Copyright (c) 2014, Joyent, Inc. All rights reserved.

var stream = require('stream');



///--- Functions

function memstream(a) {
    var r = stream.Readable({ 'objectMode': true });
    r.ended = false;
    r.i = 0;
    r._read = function () {
        if (r.ended === true) {
            return;
        }
        r.push(a[r.i]);
        r.i += 1;
        if (r.i === a.length) {
            r.ended = true;
            r.push(null);
        }
    };
    return (r);
}



///--- API

module.exports = memstream;
