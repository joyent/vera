// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');



/**
 * A raft configuration object returned from the clog has the following
 * structure (with more fields for the individual instances, depending on the
 * connection info):
 *
 * {
 *   "clogIndex": <int>,
 *   //This is present for all cluster configs after the first, making a chain
 *   // back to the entry at index 0.
 *   "prevClogIndex": <int>,
 *   //This section is only present if we aren't in the middle of a cluster
 *   // reconfiguration.
 *   "current": {
 *     "<id>": {
 *       "voting": <true/false> //false for read-only peers
 *       ...                    //Other config params, like connection info.
 *     },
 *     ...
 *   },
 *   //These sections are only present if we're in the middle of a cluster
 *   // reconfiguration.
 *   "new": { [structure same as for "current"] },
 *   "old": { [structure same as for "current"] }
 * }
 *
 * This function transforms the above into a configuration that can be used
 * by the rest of this class (TODO: We're going to need to make this an
 * object.)
 */

///--- Functions

function Cluster(opts) {
    assert.object(opts, 'opts');
    assert.string(opts.id, 'opts.id');
    assert.object(opts.clusterConfig, 'opts.clusterConfig');

    var self = this;
    self.id = opts.id;
    self.clusterConfig = opts.clusterConfig;
    self.filteredPeers = [];
    //For now, to get all the tests working before we refactor more things,
    // we just return the same structure we had previously (an array of
    // ids).  TODO: Figure out what we should be filtering down to and make
    // the object for checking majorities.
    if (self.clusterConfig.current) {
        Object.keys(self.clusterConfig.current).forEach(function (p) {
            if (p !== self.id) {
                self.filteredPeers.push(p);
            }
        });
    }
    self.maj = Math.floor(self.filteredPeers.length / 2) + 1;
}


module.exports = Cluster;



///--- API

Cluster.prototype.peerIds = function (ids) {
    var self = this;
    return (self.filteredPeers);
};


Cluster.prototype.numPeers = function () {
    var self = this;
    return (self.filteredPeers.length);
};

Cluster.prototype.peerExists = function (id) {
    var self = this;
    return (self.filteredPeers.indexOf(id) !== -1);
};


Cluster.prototype.majority = function (ids) {
    var self = this;
    return (self.maj);
};
