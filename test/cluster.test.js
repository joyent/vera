// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var Cluster = require('../lib/cluster');
var test = require('nodeunit-plus').test;



///--- Tests

test('empty config', function (t) {
    var c = new Cluster({
        'id': 'raft-0',
        'clusterConfig': {
            'clogIndex': 0,
            'current': {}
        }
    });
    t.equal(c.id, 'raft-0');
    t.equal(c.clogIndex, 0);
    t.deepEqual([], c.allIds);
    t.deepEqual([], c.allPeerIds);
    t.deepEqual([], c.votingIds);
    t.deepEqual([], c.votingPeerIds);
    t.ok(c.isMajority([ 'raft-0' ]) === false);
    t.done();
});


test('one node current', function (t) {
    var c = new Cluster({
        'id': 'raft-0',
        'clusterConfig': {
            'clogIndex': 0,
            'current': {
                'raft-0': {
                    'voting': true
                }
            }
        }
    });
    t.equal(c.id, 'raft-0');
    t.equal(c.clogIndex, 0);
    t.deepEqual([ 'raft-0' ], c.allIds);
    t.deepEqual([], c.allPeerIds);
    t.deepEqual([ 'raft-0' ], c.votingIds);
    t.deepEqual([], c.votingPeerIds);
    t.ok(c.isMajority([ 'raft-0' ]));
    t.ok(c.votingInAllConfigs('raft-0'));
    t.ok(c.votingInLatestConfig('raft-0'));
    t.done();
});


test('three node current config', function (t) {
    var c = new Cluster({
        'id': 'raft-0',
        'clusterConfig': {
            'clogIndex': 0,
            'current': {
                'raft-0': {
                    'voting': true
                },
                'raft-1': {
                    'voting': true
                },
                'raft-2': {
                    'voting': true
                }
            }
        }
    });
    t.equal(c.id, 'raft-0');
    t.equal(c.clogIndex, 0);
    t.deepEqual([ 'raft-0', 'raft-1', 'raft-2' ], c.allIds);
    t.deepEqual([ 'raft-1', 'raft-2' ], c.allPeerIds);
    t.deepEqual([ 'raft-0', 'raft-1', 'raft-2' ], c.votingIds);
    t.deepEqual([ 'raft-1', 'raft-2' ], c.votingPeerIds);
    //Time to get pedantic again.
    t.ok(c.isMajority([ 'raft-0' ]) === false);
    t.ok(c.isMajority([ 'raft-1' ]) === false);
    t.ok(c.isMajority([ 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1' ]));
    t.ok(c.isMajority([ 'raft-0', 'raft-2' ]));
    t.ok(c.isMajority([ 'raft-1', 'raft-2' ]));
    t.ok(c.isMajority([ 'raft-0', 'raft-1', 'raft-2' ]));
    t.ok(c.isMajority([ 'raft-0', 'raft-0', 'raft-0' ]) === false);
    t.ok(c.votingInAllConfigs('raft-0'));
    t.ok(c.votingInAllConfigs('raft-1'));
    t.ok(c.votingInAllConfigs('raft-2'));
    t.ok(c.votingInLatestConfig('raft-0'));
    t.ok(c.votingInLatestConfig('raft-1'));
    t.ok(c.votingInLatestConfig('raft-2'));
    t.done();
});


test('nonvoting config', function (t) {
    var c = new Cluster({
        'id': 'raft-0',
        'clusterConfig': {
            'clogIndex': 0,
            'current': {
                'raft-0': {
                    'voting': true
                },
                'raft-1': {
                    'voting': false
                },
                'raft-2': {
                    'voting': false
                },
                'raft-3': {
                    'voting': true
                }
            }
        }
    });
    t.equal(c.id, 'raft-0');
    t.equal(c.clogIndex, 0);
    t.deepEqual([ 'raft-0', 'raft-1', 'raft-2', 'raft-3' ], c.allIds);
    t.deepEqual([ 'raft-1', 'raft-2', 'raft-3' ], c.allPeerIds);
    t.deepEqual([ 'raft-0', 'raft-3' ], c.votingIds);
    t.deepEqual([ 'raft-3' ], c.votingPeerIds);
    //Time to get pedantic again.
    t.ok(c.isMajority([ 'raft-0' ]) === false);
    t.ok(c.isMajority([ 'raft-1' ]) === false);
    t.ok(c.isMajority([ 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-3' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-1', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-3' ]));
    t.ok(c.votingInAllConfigs('raft-0'));
    t.ok(c.votingInAllConfigs('raft-1') === false);
    t.ok(c.votingInAllConfigs('raft-2') === false);
    t.ok(c.votingInAllConfigs('raft-3'));
    t.ok(c.votingInLatestConfig('raft-0'));
    t.ok(c.votingInLatestConfig('raft-1') === false);
    t.ok(c.votingInLatestConfig('raft-2') === false);
    t.ok(c.votingInLatestConfig('raft-3'));
    t.done();
});


test('empty reconfiguration', function (t) {
    var c = new Cluster({
        'id': 'raft-0',
        'clusterConfig': {
            'clogIndex': 0,
            'old': {},
            'new': {}
        }
    });
    t.equal(c.id, 'raft-0');
    t.equal(c.clogIndex, 0);
    t.deepEqual([], c.allIds);
    t.deepEqual([], c.allPeerIds);
    t.deepEqual([], c.votingIds);
    t.deepEqual([], c.votingPeerIds);
    t.ok(c.isMajority([ 'raft-0' ]) === false);
    t.done();
});


test('reconfiguration', function (t) {
    var c = new Cluster({
        'id': 'raft-0',
        'clusterConfig': {
            'clogIndex': 0,
            'old': {
                'raft-0': {
                    'voting': true
                },
                'raft-1': {
                    'voting': true
                },
                'raft-2': {
                    'voting': true
                }
            },
            'new': {
                'raft-0': {
                    'voting': true
                },
                'raft-3': {
                    'voting': true
                },
                'raft-4': {
                    'voting': true
                }
            }
        }
    });
    t.equal(c.id, 'raft-0');
    t.equal(c.clogIndex, 0);
    t.deepEqual([ 'raft-0', 'raft-1', 'raft-2', 'raft-3', 'raft-4' ], c.allIds);
    t.deepEqual([ 'raft-1', 'raft-2', 'raft-3', 'raft-4' ], c.allPeerIds);
    t.deepEqual([ 'raft-0', 'raft-1', 'raft-2', 'raft-3', 'raft-4' ],
                c.votingIds);
    t.deepEqual([ 'raft-1', 'raft-2', 'raft-3', 'raft-4' ], c.votingPeerIds);
    //Time to get pedantic again.
    t.ok(c.isMajority([ 'raft-0' ]) === false);
    t.ok(c.isMajority([ 'raft-1' ]) === false);
    t.ok(c.isMajority([ 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-3' ]) === false);
    t.ok(c.isMajority([ 'raft-4' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-1', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-0', 'raft-0' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-3' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1', 'raft-3' ]));
    t.ok(c.isMajority([ 'raft-0', 'raft-2', 'raft-4' ]));
    t.ok(c.votingInAllConfigs('raft-0'));
    t.ok(c.votingInAllConfigs('raft-1') === false);
    t.ok(c.votingInAllConfigs('raft-2') === false);
    t.ok(c.votingInAllConfigs('raft-3') === false);
    t.ok(c.votingInAllConfigs('raft-4') === false);
    t.ok(c.votingInLatestConfig('raft-0'));
    t.ok(c.votingInLatestConfig('raft-1') === false);
    t.ok(c.votingInLatestConfig('raft-2') === false);
    t.ok(c.votingInLatestConfig('raft-3'));
    t.ok(c.votingInLatestConfig('raft-4'));
    t.done();
});


test('reconfiguration and nonvoting', function (t) {
    var c = new Cluster({
        'id': 'raft-0',
        'clusterConfig': {
            'clogIndex': 0,
            'old': {
                'raft-0': {
                    'voting': true
                },
                'raft-1': {
                    'voting': false
                },
                'raft-2': {
                    'voting': false
                }
            },
            'new': {
                'raft-0': {
                    'voting': false
                },
                'raft-3': {
                    'voting': true
                },
                'raft-4': {
                    'voting': true
                }
            }
        }
    });
    t.equal(c.id, 'raft-0');
    t.equal(c.clogIndex, 0);
    t.deepEqual([ 'raft-0', 'raft-1', 'raft-2', 'raft-3', 'raft-4' ], c.allIds);
    t.deepEqual([ 'raft-1', 'raft-2', 'raft-3', 'raft-4' ], c.allPeerIds);
    t.deepEqual([ 'raft-0', 'raft-3', 'raft-4' ], c.votingIds);
    t.deepEqual([ 'raft-3', 'raft-4' ], c.votingPeerIds);
    //Time to get pedantic again.
    t.ok(c.isMajority([ 'raft-0' ]) === false);
    t.ok(c.isMajority([ 'raft-1' ]) === false);
    t.ok(c.isMajority([ 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-3' ]) === false);
    t.ok(c.isMajority([ 'raft-4' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-1', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1', 'raft-2' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-0', 'raft-0' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-3' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-1', 'raft-3' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-2', 'raft-4' ]) === false);
    t.ok(c.isMajority([ 'raft-0', 'raft-3', 'raft-4' ]));
    t.ok(c.votingInAllConfigs('raft-0') === false);
    t.ok(c.votingInAllConfigs('raft-1') === false);
    t.ok(c.votingInAllConfigs('raft-2') === false);
    t.ok(c.votingInAllConfigs('raft-3') === false);
    t.ok(c.votingInAllConfigs('raft-3') === false);
    t.ok(c.votingInLatestConfig('raft-0') === false);
    t.ok(c.votingInLatestConfig('raft-1') === false);
    t.ok(c.votingInLatestConfig('raft-2') === false);
    t.ok(c.votingInLatestConfig('raft-3'));
    t.ok(c.votingInLatestConfig('raft-4'));
    t.done();
});
