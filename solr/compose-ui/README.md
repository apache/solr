# Compose Admin UI

> **⚠️ EXPERIMENTAL ⚠️**
>
> This is an experimental module developed as a proof-of-concept. Many parts of the UI
> are under development, use wrong colors, may not work or have limited functionality.

This module contains the code for the new Admin UI written in Kotlin / Compose Multiplatform.

## Supported Targets

The module is available for desktop / JVM targets and web (WebAssembly).

## Build and Run

To build and run the desktop client simply run `./gradlew :solr:compose-ui:run`.

Make sure that you have a Solr development instance running on `localhost:8983`, as the current
implementation uses hardcoded values.

The WebAssembly app is built and deployed together with the current webapp. To run and access it,
you can build the project as usual (see [Quickstart](../../README.md#quickstart)) and then access
[`http://localhost:8983/solr/compose`](http://localhost:8983/solr/compose).

Various references are included in the webapp for already migrated pages.

> Note that the standalone WebAssembly app executed via
> `./gradlew :solr:compose-ui:wasmJsBrowserRun` runs on port `8080` and will run
> into CORS exceptions. Therefore, the usage of it for development is
> discouraged.
>
> Consider one of the above options instead.
