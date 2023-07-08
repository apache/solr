#!/usr/bin/env bats

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

# This file has stubs for setup and teardown of a cluster and debugging hints

load bats_helper

setup_file() {
  # set up paths and helpers
  common_clean_setup

  solr start -c -V
  # echo $output >&3
}

teardown_file() {
  # set up paths, not preserved from setup
  common_setup
  sleep 3

  # Conversely, on shutdown, we do need this to execute strictly
  # because using "run" will eat filing test exit codes
  solr stop -all
  # DEBUG : (echo -n "# " ; solr stop -V -all) >&3
}

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure
}

@test "nothing" {
  # hint: if we need to demonstrate a failing test, change this line to 'false'
  true
}
