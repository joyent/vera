#!/bin/bash
###############################################################################
# Copyright 2013 Joyent, Inc., All rights reserved.
# Runs nodeunit for each *.test.js
###############################################################################

set -o pipefail

for t in $(find test -name '*.test.js'); do
    ./node_modules/nodeunit/bin/nodeunit $t
    if [[ $? -ne 0 ]]; then
        exit
    fi
done
