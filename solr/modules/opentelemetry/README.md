<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Apache Solr Open Telemetry Tracer
=====================================

Introduction
------------
This module brings support for the new [OTEL](https://opentelemetry.io) (OpenTelemetry) standard,
and exposes a tracer configurator that can be enabled in the
`<tracerConfig>` tag of `solr.xml`. Please see Solr Reference Guide chapter "Distributed Tracing"
for details.

Enabling Tracing in Tests
--------------------------

Here's a tip to enable distributed tracing in a Solr test:


### Step 1: Download the OpenTelemetry Java Agent

Run the Gradle task to download the agent JAR:

```bash
./gradlew :solr:modules:opentelemetry:downloadOtelAgent
```

This downloads the agent to: `solr/modules/opentelemetry/build/agent/opentelemetry-javaagent.jar`

The file is gitignored (build directory) so you'll need to run this task once per checkout or after clean builds.

### Step 2: Set Up a Tracing Backend (e.g., Jaeger)

Start Jaeger using Docker:

```bash
docker run --rm --name jaeger \
  -p 16686:16686 -p 4317:4317 \
  cr.jaegertracing.io/jaegertracing/jaeger:2.10.0
```

Access Jaeger UI at: http://localhost:16686

### Step 3: Configure IntelliJ Run Configuration

1. **Open Run/Debug Configurations**
    - Click on the configuration dropdown in the toolbar
    - Select "Edit Configurations..."

2. **Select or Create a Test Configuration**
    - Select an existing JUnit test configuration, or
    - Click "+" to create a new JUnit configuration

3. **Add VM Options**
    - In the "VM options" field, add:
   ```
   -javaagent:$PROJECT_DIR$/solr/modules/opentelemetry/build/agent/opentelemetry-javaagent.jar
   -Dotel.javaagent.configuration-file=$PROJECT_DIR$/solr/modules/opentelemetry/otel-agent-test-config.properties
   ```

4. **Apply and Save**
    - Click "Apply" then "OK"

### Step 4: Run Tests with Tracing

1. Run your test configuration as normal
2. Open Jaeger UI at http://localhost:16686
3. Select service "solr-test" from the dropdown
4. Click "Find Traces" to see your test execution traces
