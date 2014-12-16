/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

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
