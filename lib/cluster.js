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
    assert.number(opts.clusterConfig.clogIndex, 'opts.clusterConfig.clogIndex');
    assert.optionalNumber(opts.clusterConfig.prevClogIndex,
                          'opts.clusterConfig.prevClogIndex');

    //We don't assert that the id passed in is part of the configuration since
    // the end stage of a configuration flip is a leader stepping down.  It
    // (the host) needs to know it isn't part of the configuration any more.

    var self = this;
    self.reconfiguration = (opts.clusterConfig.current === undefined);

    self.id = opts.id;
    self.clogIndex = opts.clusterConfig.clogIndex;
    self.clusterConfig = opts.clusterConfig;
    //All unique ids (including self)
    self.allIds = undefined;
    //Peer unique ids
    self.allPeerIds = undefined;
    //Voting members
    self.votingIds = undefined;
    //Peer voting members
    self.votingPeerIds = undefined;

    //Make sure everything else checks out and set up the above.
    if (self.reconfiguration) {
        assert.object(opts.clusterConfig.old, 'opts.clusterConfig.old');
        assert.object(opts.clusterConfig.new, 'opts.clusterConfig.new');
        self.old = new Config(self.id, opts.clusterConfig.old);
        self.new = new Config(self.id, opts.clusterConfig.new);
        self.allIds = unique(self.old.allIds,
                             self.new.allIds);
        self.allPeerIds = unique(self.old.allPeerIds,
                                 self.new.allPeerIds);
        self.votingIds = unique(self.old.votingIds,
                                self.new.votingIds);
        self.votingPeerIds = unique(self.old.votingPeerIds,
                                    self.new.votingPeerIds);
    } else {
        assert.object(opts.clusterConfig.current, 'opts.clusterConfig.current');
        self.current = new Config(self.id, opts.clusterConfig.current);
        self.allIds = self.current.allIds;
        self.allPeerIds = self.current.allPeerIds;
        self.votingIds = self.current.votingIds;
        self.votingPeerIds = self.current.votingPeerIds;
    }
    self.numPeers = self.allPeerIds.length;
}

module.exports = Cluster;



///--- Inner classes

function Config(id, config) {
    assert.string(id, 'id');
    assert.object(config, 'config');

    var self = this;
    self.allIds = [];
    self.allPeerIds = [];
    self.votingIds = [];
    self.votingPeerIds = [];
    Object.keys(config).forEach(function (pid) {
        var c = config[pid];
        if (self.allIds.indexOf(pid) === -1) {
            self.allIds.push(pid);
        }
        if (id !== pid && self.allPeerIds.indexOf(pid) === -1) {
            self.allPeerIds.push(pid);
        }
        if (c.voting && self.votingIds.indexOf(pid) === -1) {
            self.votingIds.push(pid);
        }
        if (id !== pid && c.voting && self.votingPeerIds.indexOf(pid) === -1) {
            self.votingPeerIds.push(pid);
        }
    });
    self.majority = Math.floor(self.votingIds.length / 2) + 1;
}


/**
 * Returns true if the ids are in the majority of voting peers.
 */
Config.prototype.isMajority = function (ids) {
    var self = this;
    var u = [];
    ids.forEach(function (id) {
        if (self.votingIds.indexOf(id) !== -1 &&
            u.indexOf(id) === -1) {
            u.push(id);
        }
    });
    return (u.length >= self.majority);
};



///--- Helpers

function unique() {
    var u = [];
    for (var i = 0; i < arguments.length; ++i) {
        arguments[i].forEach(function (s) {
            if (u.indexOf(s) === -1) {
                u.push(s);
            }
        });
    }
    return (u);
}



///--- API

Cluster.prototype.isMajority = function (ids) {
    var self = this;
    //In a reconfiguration, majority is only granted when there is a majority
    // from both the old and the new configurations.
    if (self.reconfiguration) {
        return (self.old.isMajority(ids) && self.new.isMajority(ids));
    } else {
        return (self.current.isMajority(ids));
    }
};


Cluster.prototype.exists = function (id) {
    var self = this;
    return (self.allIds.indexOf(id) !== -1);
};


Cluster.prototype.peerExists = function (id) {
    var self = this;
    return (self.allPeerIds.indexOf(id) !== -1);
};


Cluster.prototype.get = function (id) {
    var self = this;
    return (self.clusterConfig.current[id]);
};


Cluster.prototype.votingInAllConfigs = function (id) {
    var self = this;
    if (self.reconfiguration) {
        return (self.old.votingIds.indexOf(id) !== -1 &&
                self.new.votingIds.indexOf(id) !== -1);
    } else {
        return (self.current.votingIds.indexOf(id) !== -1);
    }
};


Cluster.prototype.votingInLatestConfig = function (id) {
    var self = this;
    if (self.reconfiguration) {
        return (self.new.votingIds.indexOf(id) !== -1);
    } else {
        return (self.current.votingIds.indexOf(id) !== -1);
    }
};
