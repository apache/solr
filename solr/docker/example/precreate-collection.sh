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

# A script that creates a core by copying config before starting solr.
#
# To use this, map this file into your container's docker-entrypoint-initdb.d directory:
#
#     docker run -d -P -v $PWD/precreate-collection.sh:/docker-entrypoint-initdb.d/precreate-collection.sh solr

CORE=${CORE:-gettingstarted}
if [[ -d "/opt/solr/server/solr/$CORE" ]]; then
    echo "$CORE is already present on disk"
    exit 0
fi

mkdir -p "/opt/solr/server/solr/$CORE/"
cd "/opt/solr/server/solr/$CORE" || exit
touch core.properties
# TODO: we may want a more minimal example here
cp -r /opt/solr/example/files/* .
echo created "/opt/solr/server/solr/$CORE"
