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
> This is an experimental module because of its early state. Many parts of the UI
> are under development and may not work or have limited functionality.

This module contains the code for the new Admin UI written in Kotlin / Compose Multiplatform.

## Supported Targets

The module is available for desktop / JVM and web (WebAssembly).

## Build and Run

> **IMPORTANT**
>
> Before you try to build the project, make sure you update your `gradle.properties` file to
> reflect the module's configuration requirements. Review the differences between your file and
> `gradle/template.gradle.properties` and update accordingly.

This module is provided as an independent sub-module. Therefore, if you use the terminal you have to prefix any
task targeting this module with `-p ui`, like `./gradlew -p ui check`.

If you load the module in your IDE, the IDE may create a new gradle wrapper under `ui/gradle/wrapper` and
gradlew/gradle.bat files under `ui/`. In this case make sure the wrapper is on the same version as the root project
(see `gradle/wrapper/gradle-wrapper.properties`) to avoid any build issues later on CI.

To build and run the desktop client simply run from the root directory `./gradlew -p :desktopApp:run`, or if you
loaded the module separately from inside `ui/` (new root) `./gradlew :desktopApp:run`.

The desktop app is running a standalone client and therefore need a solr instance / backend to connect with.

> Note: The current implementation may be limited to run only with locally hosted solr instances.

The WebAssembly app is built and published at [Apache nightlies](https://nightlies.apache.org/solr/ui/wasm/)
and bundled during the Solr build (see [README.md](../README.md)) into the current webapp.
You can choose the built to include by setting `solr.ui.commit` in the `gradle.properties`,
or build from source by setting `solr.ui.buildFromSource=true`. Once the Solr project then access
[`http://127.0.0.1:8983/solr/ui/`](http://127.0.0.1:8983/solr/ui/).

Various references are included in the webapp for already migrated pages.

> Note that the standalone WebAssembly app executed via
> `./gradlew :webApp:wasmJsBrowserRun` runs on port `8080` and will run
> into CORS exceptions. Therefore, the usage of it for development is
> discouraged.
>
> Consider one of the above options instead.
