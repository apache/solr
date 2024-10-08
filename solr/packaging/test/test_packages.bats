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

teardown() {
  # save a snapshot of SOLR_HOME for failed tests
  save_home_on_failure

  solr stop -all >/dev/null 2>&1
}

@test "package detects no running solr" {
  # not sure this is actually a good thing..  we may not want this..
  run solr package
  assert_output --partial "No Solr nodes are running."
}

@test "lifecycle of package" {
  run solr start -c -Denable.packages=true

  run solr package
  refute_output --partial "No Solr nodes are running."

  run solr package -help
  assert_output --partial "Add a repository to Solr"

  run solr package list-available
  assert_output --partial "Available packages:"
}

@test "deploying and undeploying a collection level package" {
  run solr start -c -Denable.packages=true

  solr create_collection -c foo-1.2

  # Deploy package - the package doesn't need to exist before the collection validation kicks in
  run solr package deploy PACKAGE_NAME -collections foo-1.2
  # assert_output --partial "Deployment successful"
  refute_output --partial "Invalid collection"
  
  # Until PACKAGE_NAME refers to an actual installable package, this is as far as we get.
  assert_output --partial "Package instance doesn't exist: PACKAGE_NAME:null"

  # Undeploy package
  run solr package undeploy PACKAGE_NAME -collections foo-1.2
  refute_output --partial "Invalid collection"
  assert_output --partial "Package PACKAGE_NAME not deployed on collection foo-1.2"
}

# This test is useful if you are debugging/working with packages.
# We have commented it out for now since it depends on a live internet
# connection to run.  This could be updated with a local Repo server if we had
# a package that is part of the Solr project to use.
# @test "deploying and undeploying a cluster level package" {
#  run solr start -c -Denable.packages=true
  
#  run solr package add-repo splainer "https://raw.githubusercontent.com/o19s/splainer/main/solr-splainer-package/repo"
#  assert_output --partial "Added repository: splainer"
  
#  run solr package list-available
#  assert_output --partial "solr-splainer 		Splainer for Solr"
#  run solr package install solr-splainer
#  assert_output --partial "solr-splainer installed."

#  run solr package deploy solr-splainer -y -cluster
#  assert_output --partial "Deployment successful"
  
#  run -0 curl --fail http://localhost:${SOLR_PORT}/v2/splainer/index.html
# }
