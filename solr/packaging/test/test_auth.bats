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

load bats_helper

setup() {
  common_clean_setup

  run solr auth disable
}

# Note: there are additional auth related tests in test_ssl.bats

@test "auth rejects blockUnknown option with invalid boolean" {
  run ! solr auth enable -type basicAuth -credentials any:any -blockUnknown ture
  assert_output --partial "Argument [blockUnknown] must be either true or false, but was [ture]"
}

@test "auth rejects updateIncludeFileOnly option with invalid boolean" {
  run ! solr auth enable -type basicAuth -credentials any:any -updateIncludeFileOnly ture
  assert_output --partial "Argument [updateIncludeFileOnly] must be either true or false, but was [ture]"
}
