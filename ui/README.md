<!--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
 -->

# Compose Admin UI

> **⚠️ EXPERIMENTAL ⚠️**
>
> This is an experimental module developed as a proof-of-concept. Many parts of the UI
> are under development, use wrong colors, may not work or have limited functionality.

This module contains the code for the new Admin UI written in Kotlin / Compose Multiplatform.

## Supported Targets

The module is available for desktop / JVM targets and web (WebAssembly).

## Build and Run

> **IMPORTANT**
> 
> Before you try to build your project, make sure you update your `gradle.properties` files to
> reflect the module's configuration requirements. Review the differences between your file and
> `gradle/template.gradle.properties` and update accordingly.

To build and run the desktop client simply run `./gradlew :solr:ui:run`.

Make sure that you have a Solr development instance running on `localhost:8983`, as the current
implementation uses hardcoded values.

The WebAssembly app is built and deployed together with the current webapp. To run and access it,
you can build the project as usual (see [Quickstart](../../README.md#quickstart)) and then access
[`http://localhost:8983/solr/compose`](http://localhost:8983/solr/compose).

Various references are included in the webapp for already migrated pages.

> Note that the standalone WebAssembly app executed via
> `./gradlew :solr:ui:wasmJsBrowserRun` runs on port `8080` and will run
> into CORS exceptions. Therefore, the usage of it for development is
> discouraged.
>
> Consider one of the above options instead.
