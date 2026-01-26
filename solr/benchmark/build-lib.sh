#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds all JARs and copies them to lib/ so jmh.sh can run without invoking gradle.

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT="$SCRIPT_DIR/../.."
LIB_DIR="$SCRIPT_DIR/lib"

echo "Building jars..."
"$REPO_ROOT/gradlew" -p "$REPO_ROOT" jar

echo "Getting classpath from gradle..."
CLASSPATH=$("$REPO_ROOT/gradlew" -p "$SCRIPT_DIR" -q echoCp)

echo "Copying JARs to lib/..."
rm -rf "$LIB_DIR"
mkdir -p "$LIB_DIR"

# Copy all JAR dependencies
echo "$CLASSPATH" | tr ':' '\n' | while read -r jar; do
  if [ -f "$jar" ] && [[ "$jar" == *.jar ]]; then
    cp "$jar" "$LIB_DIR/"
  fi
done

# Copy the benchmark module's own JAR (echoCp outputs classes dir, not the JAR)
BENCHMARK_JAR=$(ls "$SCRIPT_DIR/build/libs"/solr-benchmark-*.jar 2>/dev/null | head -1)
if [ -n "$BENCHMARK_JAR" ]; then
  cp "$BENCHMARK_JAR" "$LIB_DIR/"
fi

JAR_COUNT=$(ls -1 "$LIB_DIR"/*.jar 2>/dev/null | wc -l)
echo "Done. Copied $JAR_COUNT JARs to lib/"
echo "You can now run jmh.sh without gradle being invoked."
