#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o pipefail

for t in $(find test -name '*.test.js'); do
    ./node_modules/nodeunit/bin/nodeunit $t
    if [[ $? -ne 0 ]]; then
        exit
    fi
done
