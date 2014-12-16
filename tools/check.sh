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
set -o xtrace

cd deps/javascriptlint && make install && cd -

JSL=deps/javascriptlint/build/install/jsl
JSSTYLE=deps/jsstyle/jsstyle
JS_FILES=$(echo $(ls *.js; find bin lib test -name '*.js') | tr '\n' ' ')

$JSL --nologo --nosummary --conf=./tools/jsl.node.conf $JS_FILES
if [[ $? != 0 ]]; then
    exit 1
fi

$JSSTYLE -f tools/jsstyle.conf $JS_FILES
if [[ $? != 0 ]]; then
    exit 1
fi
