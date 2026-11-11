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

Assembles the Solr binary distribution, and hosts the tests that drive `bin/solr` and `bin\solr.cmd` — they need an unpacked distribution, which is why they live here.

No production Java of its own. `README.txt` in this directory ships *inside* the distribution and is not developer documentation.

## Building a distribution

| Task | Output |
| --- | --- |
| `installFullDist` / `installSlimDist` | `build/solr-<version>` / `build/solr-<version>-slim` |
| `fullDistTar` / `slimDistTar` | `build/distributions/solr-<version>[-slim].tgz` |
| `devFull` / `devSlim` / `dev` | a copy at `build/dev` / `build/dev-slim`, for working by hand |

Slim is `bin`, `server`, `docker`, `lib` and licenses; full adds `example`, `modules` and `cross-dc-manager`. Zip variants are disabled. `assembleDist` also requires `-Psolr.ui.buildFromSource=true`, so a release never pulls the Admin UI from nightlies.

```bash
./gradlew -p solr/packaging devSlim
cd solr/packaging/build/dev-slim && bin/solr start -f
```

The test tasks use `build/solr-<version>`, so a `dev` directory you are poking at is never disturbed by a test run.

## Tests

| Layer | Task | Status |
| --- | --- | --- |
| CATS (JUnit, cross-platform) | `catsTests`, `catsTestsPicocli`, `catsTestsCommonsCli` | preferred |
| Rolling upgrade (Docker) | `catsTestsDockerBuild` | builds the image first, so it has its own task |
| Everything, in parallel | `catsTestsAll` | the above in one fork pool; builds the image too |
| BATS suites (bash) | `integrationTests`, `integrationTestsPicocli` | deprecated, being ported to CATS |
| Stock `test` | — | disabled here |

The real layers all start Solr nodes and take minutes, so none of them is in `check`; ask for them by name. The `*CommonsCli` and `*Picocli` variants pin `SOLR_PICOCLI`, since the two CLI implementations still differ in some wording.

**CATS** — Cross-platform Automated Testing System. JUnit suites in `src/test/org/apache/solr/shell/` extending `SolrShellTestCase`. One test source runs against `bin/solr` under bash and `bin\solr.cmd` under cmd.exe. Each test class gets its own `SOLR_HOME`, `SOLR_PID_DIR` and port block, so classes run in parallel forks (`min(4, tests.jvms)`, override with `-Pcats.tests.jvms`). See [`solr/test-framework/README.md`](../test-framework/README.md) for the API.

```bash
./gradlew -p solr/packaging catsTests --tests "*ZkTest"
```

`RollingUpgradeTest` upgrades a three-node cluster from the previous major's released image to one built from your working tree, so it needs that image and runs only from `catsTestsDockerBuild` or `catsTestsAll`. Prefer `catsTestsAll` when you want the whole suite: tasks within a project run sequentially, so asking for both of the others runs the upgrade after everything else instead of alongside it. Nothing else triggers a Docker build — not `catsTests`, not `check` — and the task tags the image under a local-only name, so an `apache/solr:<version>` you pulled from Docker Hub is never overwritten.

**BATS** — `test/*.bats`. Deprecated in favour of CATS: bash-only, so they have never run on Windows. `downloadBats` fetches bats-core and its helpers for you; `--tests <file>.bats` selects one file; tests get a random port, pinnable with `-Pbats.port`; `# bats test_tags=nightly` tests need `-Psolr.bats.nightly=true`. Touch these only to keep them working, or to delete one once its port is in place.

New CLI coverage goes in CATS. If the thing can be tested without spawning a script, test it as an ordinary JUnit test instead of either layer.

## When something fails

| | CATS | BATS |
| --- | --- | --- |
| Output root | `build/cats-test-output[-picocli|-commons-cli]` | `build/test-output[-picocli]` |
| JUnit XML | `build/test-results/<task>` | the output root |
| Failure snapshot | `<root>/failure-snapshots/<Class>-<method>/`, with `SOLR_HOME` and `transcript.log` | `<root>/failure-snapshots/` |

The harness prints the snapshot path for each failed test, and `transcript.log` holds every command with its output. Output roots are wiped at the start of each run.

A CATS run prints one line per test as it finishes (`ok`, `skip` or `FAIL`, with the class name because forks interleave). `build/test-results/<task>/*.xml` is only written when the task ends, so it is no use for watching a run in flight.

CI runs CATS on pull requests that touch `solr/bin`, this module or the CLI, from `.github/workflows/cats-test.yml`. The BATS workflow, `.github/workflows/bin-solr-test.yml`, is manual-only now that every suite has a CATS counterpart.

## See also

- [`solr/test-framework/README.md`](../test-framework/README.md) — the harness API and the other test base classes.
- [`dev-docs/writing-tests.adoc`](../../dev-docs/writing-tests.adoc) — which kind of test to write.
