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

set -e

base_dir=$(dirname "$0")

if [ "${base_dir}" == "." ]; then
  gradlew_dir="../.."
else
  echo "Benchmarks need to be run from the 'solr/benchmark' directory"
  exit
fi


if [ -d "lib" ]
then
  echo "Using lib directory for classpath..."
  classpath="lib/*:build/classes/java/main"
else
  echo "Getting classpath from gradle..."
  # --no-daemon
  gradleCmd="${gradlew_dir}/gradlew"
  $gradleCmd -q -p ../../ jar
  echo "gradle build done"
  classpath=$($gradleCmd -q echoCp)
fi

# shellcheck disable=SC2145
echo "running JMH with args: $@"


# -XX:+PreserveFramePointer is not necessary with async profiler and they claim can be up to 10% hit

# -XX:+UseStringDeduplication should experiment with this
# -XX:MaxMetaspaceExpansion=64M  # and this note:  Avoids triggering full GC when we just allocate a bit more metaspace, and metaspace automatically gets cleaned anyway.
#                                                  MRM: Metaspace should default to 1GB with compressed ops on I believe, but I think even that is low for Solr in this scenario.
# -XX:+UnlockExperimentalVMOptions -XX:G1NewSizePercent=20  # and this note: Prevents G1 undermining young gen, which otherwise causes a cascade of issues
#                                                            MRM: I've also seen 15 claimed as a sweet spot.

# -XX:-UseBiasedLocking - should be unreflective in recent JVMs and removed in the latest.

jvmArgs="-jvmArgs -Djmh.shutdownTimeout=5 -jvmArgs -Djmh.shutdownTimeout.step=3 -jvmArgs -Djava.security.egd=file:/dev/./urandom  -jvmArgs -XX:-UseBiasedLocking -jvmArgs -XX:+UnlockDiagnosticVMOptions -jvmArgs -XX:+DebugNonSafepoints -jvmArgs --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
gcArgs="-jvmArgs -XX:+UseG1GC -jvmArgs -XX:+ParallelRefProcEnabled"

# -jvmArgs -Dlog4j2.debug 
loggingArgs="-jvmArgs -Dlog4jConfigurationFile=./log4j2-bench.xml -jvmArgs -Dlog4j2.is.webapp=false -jvmArgs -Dlog4j2.garbagefreeThreadContextMap=true -jvmArgs -Dlog4j2.enableDirectEncoders=true -jvmArgs -Dlog4j2.enable.threadlocals=true"

#set -x

# shellcheck disable=SC2086
exec java -cp "$classpath" --add-opens=java.base/java.io=ALL-UNNAMED -Djdk.module.illegalAccess.silent=true org.openjdk.jmh.Main $jvmArgs $loggingArgs $gcArgs "$@"

echo "JMH benchmarks done"
