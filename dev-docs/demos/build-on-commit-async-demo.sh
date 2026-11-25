#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Demonstrates SOLR-18348: the worst case (a suggester, a spellchecker, and filterCache
# autowarming all slow and all synchronous, so a single commit pays for all three back to
# back), then the improvement from buildOnCommitAsync=true (suggester + spellchecker rebuilds
# move to the background - filterCache autowarming does not yet have an async option, so it
# still contributes to the "after" number; see BuildOnCommitAsyncDemo's class javadoc).
#
# This runs a real embedded Solr core (via the same test infrastructure the rest of the test
# suite uses) in-process - no external bin/solr instance or network calls are involved - and
# prints a before/after timing report to the terminal.
#
# Usage: dev-docs/demos/build-on-commit-async-demo.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

echo "Running BuildOnCommitAsyncDemo (this takes ~15-20s: it deliberately runs the slow-build" \
     "scenario first)..."
echo

# -i (--info) is what makes Gradle forward the test's System.out to this terminal; everything
# else is filtered out below so only the demo's own [DEMO]-prefixed report lines show up. Set
# DEMO_VERBOSE=1 to see the full Gradle/Solr log output instead (useful if something looks
# wrong and you want to see why).
if [[ "${DEMO_VERBOSE:-0} " == "1 " ]]; then
  exec ./gradlew :solr:core:test \
    --tests "org.apache.solr.handler.component.BuildOnCommitAsyncDemo" -i
else
  ./gradlew :solr:core:test \
    --tests "org.apache.solr.handler.component.BuildOnCommitAsyncDemo" -i \
    | grep --line-buffered -E '\[DEMO\]|BUILD (SUCCESSFUL|FAILED)'
fi
