#!/bin/bash
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

# This script is mainly an illustration for the docker-entrypoint-initdb.d extension mechanism.
# Run it with e.g.:
#
#   docker run -d -P -v $PWD/docs/set-heap.sh:/docker-entrypoint-initdb.d/set-heap.sh solr
#
# The SOLR_HEAP configuration technique here is usable for older versions of Solr.
# From Solr 6.3 setting the SOLR_HEAP can be done more easily with:
#
#   docker run -d -P -e SOLR_HEAP=800m apache/solr
#
set -e
cp /opt/solr/bin/solr.in.sh /opt/solr/bin/solr.in.sh.orig
sed -e 's/SOLR_HEAP=".*"/SOLR_HEAP="1024m"/' </opt/solr/bin/solr.in.sh.orig >/opt/solr/bin/solr.in.sh
grep '^SOLR_HEAP=' /opt/solr/bin/solr.in.sh
