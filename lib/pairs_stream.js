// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var util = require('util');
var stream = require('stream');

/**
 * Starting with two object streams, will consume both streams, object by
 * object.  Once one of the streams end, that "side" of the object stream
 * will become undefined.  For example, given these two "streams":
 * left:  [1, 2, 3]
 * right: [a, b, c, d]
 *
 * These are the pairs that will get read or emitted from the stream:
 * { 'left': 1, 'right': a }
 * { 'left': 2, 'right': b }
 * { 'left': 3, 'right': c }
 * { 'right': d }
 *
 * When this stream finishes, *both* streams will be completely consumed.
 */

function PairsStream(opts) {
    var self = this;
    self.left = opts.left;
    self.right = opts.right;
    self.leftEnded = false;
    self.rightEnded = false;
    self.waitingForReadable = false;
    self.ended = false;
    stream.Readable.call(self, { 'objectMode': true });
    init.call(self);
}

util.inherits(PairsStream, stream.Readable);
module.exports = PairsStream;



//--- Helpers

function init() {
    var self = this;

    function end() {
        if (!self.leftEnded || !self.rightEnded) {
            return;
        }
        cleanup();
        self.ended = true;
        self.push(null);
    }

    function onRightEnd() {
        self.emit('rightEnded');
        self.rightEnded = true;
        end();
    }

    function waitForRightReadable(ldata) {
        //Since we're holding onto left data, we need to be sure that we put it
        // back into the left stream before we end.  Otherwise, we could lose
        // some data from the left side.  We use this waiting for readable to
        // guard against ending before we have a chance to shift it back on.
        self.waitingForReadable = true;
        //One of two things is going to happen:
        // 1) We're waiting for more data from the right stream, in which
        //    case we'll get a readable event.
        // 2) We're at the end but haven't received the 'end' event yet.
        function onRightInnerEnd() {
            self.waitingForReadable = false;
            var canResume = self.push({ 'left': ldata });
            if (!self.leftEnded && canResume) {
                self.left.resume();
            }
            onRightEnd();
        }

        function onRightReadable() {
            self.waitingForReadable = false;
            self.right.once('end', onRightEnd);
            self.right.removeListener('end', onRightInnerEnd);
            var rdata = self.right.read();
            //For whatever reason, even though we get a 'readable' event, we
            // can read null.  I've only seen it happen between the last element
            // being read and the end event.
            if (rdata === null) {
                waitForRightReadable(ldata);
            } else {
                if (self.push({ 'left': ldata, 'right': rdata })) {
                    self.left.resume();
                }
            }
        }
        self.right.once('end', onRightInnerEnd);
        self.right.once('readable', onRightReadable);

        //We temporarily remove the 'regular' right end listener
        self.right.removeListener('end', onRightEnd);
    }

    function onLeftData(ldata) {
        if (self.rightEnded) {
            if (!self.push({ 'left': ldata })) {
                self.left.pause();
            }
            return;
        }

        var rdata = self.right.read();

        //Right was read right away, so we can just return.
        if (rdata !== null) {
            if (!self.push({ 'left': ldata, 'right': rdata })) {
                self.left.pause();
            }
            return;
        }

        //Otherwise, so we need to wait for the right to catch up.
        self.left.pause();
        waitForRightReadable(ldata);
    }

    function onError(err) {
        cleanup();
        self.emit('error', err);
    }

    function onLeftEnd() {
        self.emit('leftEnded');
        self.leftEnded = true;
        if (!self.rightEnded) {
            function onReadable() {
                var r;
                while (null !== (r = self.right.read())) {
                    //There's nothing to do about the backpressure here.  We
                    // just read as much as we can to get rid of it as fast as
                    // we can.
                    self.push({ 'right': r });
                }
            }
            self.right.on('readable', onReadable);
            onReadable();
            //The onRightEnd will take care of actually ending us...
        } else {
            end();
        }
    }

    //Right we leave in non-flowing mode.
    self.right.once('error', onError);
    self.right.once('end', onRightEnd);

    //Left goes into flowing mode.  Note adding the left listeners have to
    // be added before the right listeners, otherwise we may attempt to read
    // from the right before adding it's listeners, causing this to go into
    // "waiting for right" mode, which mucks with right's listeners.
    self.left.on('data', onLeftData);
    self.left.once('error', onError);
    self.left.once('end', onLeftEnd);

    function cleanup() {
        //We leave the onEnd listeners so that we can capture the right
        // state before the caller receives the 'end' event for this.
        self.left.removeListener('data', onLeftData);
        self.left.removeListener('error', onError);
        self.right.removeListener('error', onError);
    }
}



//--- API

PairsStream.prototype._read = function () {
    var self = this;
    if (self.ended || self.rightEnded || self.leftEnded ||
        self.waitingForReadable) {
        return;
    }
    self.left.resume();
};
