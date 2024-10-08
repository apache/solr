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
}

@test "--version and -v both return Solr version" {
  run solr --version
  assert_output --partial "Solr version is:"
  
  run solr -v
  assert_output --partial "Solr version is:"
  
}

@test "-version and version both return Solr version and deprecation" {
  run solr -version
  assert_output --partial "Solr version is:"
  assert_output --partial "Deprecated operation as of 9.8.  Please use bin/solr --version."
  
  run solr version
  assert_output --partial "Solr version is:"
  assert_output --partial "Deprecated operation as of 9.8.  Please use bin/solr --version."
  
}
