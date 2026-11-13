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

# solr/packaging

Assembles the Solr binary distribution, and hosts the tests that need an unpacked one: CATS drives `bin/solr` and `bin\solr.cmd`, and so does anything else that only reproduces against a real installation — a jar missing from the classpath, a Jetty setting, a file in the wrong place.

No production Java of its own. `README.txt` in this directory ships *inside* the distribution and is not developer documentation.

## Building a distribution

| Task | Output |
| --- | --- |
| `installFullDist` / `installSlimDist` | `build/solr-<version>` / `build/solr-<version>-slim` |
| `fullDistTar` / `slimDistTar` | `build/distributions/solr-<version>[-slim].tgz` |
| `devFull` / `devSlim` / `dev` | a copy at `build/dev` / `build/dev-slim`, for working by hand |

Full adds the examples, modules and cross-dc-manager that slim leaves out. Zip variants are disabled. The test tasks use `build/solr-<version>`, so a `dev` directory you are poking at is never disturbed by a test run.

```bash
./gradlew -p solr/packaging devSlim
cd solr/packaging/build/dev-slim && bin/solr start -f
```

## Tests

CATS suites in `src/test/org/apache/solr/shell/`, the deprecated BATS suites in `test/*.bats`. Neither is in `check`: both need an unpacked distribution and start real nodes, so ask for them by name — `catsTests` and `integrationTests`, plus the `catsTestsPicocli`/`catsTestsCommonsCli`/`catsTestsAll` variants `gradlew -p solr/packaging tasks` lists. CATS classes are isolated by `SOLR_HOME` and port block and run in parallel; `-Pcats.tests.jvms=N` sets the fork count. Output, per-test transcripts and failure snapshots land under `build/cats-test-output*` and `build/test-output*`, wiped at the start of each run.

CI runs CATS on pull requests touching `solr/bin`, this module or the CLI (`.github/workflows/cats-test.yml`).

## See also

- [`dev-docs/writing-tests.adoc`](../../dev-docs/writing-tests.adoc) — which kind of test to write, how to run each, what to read when one fails.
- [`solr/test-framework/README.md`](../test-framework/README.md) — the CATS harness API.
