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

This module assembles the Solr binary distribution. It compiles no production Java of its own: it collects the `packaging` configuration of `solr/server`, `solr/example`, `solr/modules/*`, `solr/cross-dc-manager` and `solr/docker`, adds `solr/bin`, the licenses and the top-level `LICENSE.txt`/`NOTICE.txt`/`CHANGELOG.md`, and lays the result out as the directory tree users unpack. `README.txt` in this directory is the README that ships inside that tree (its `@SOLR_DOC_URL@` and `@SOLR_CHANGES_URL@` tokens are substituted at copy time) — it is a distribution file, not developer documentation.

It is also where the tests that drive `bin/solr` and `bin\solr.cmd` live, because those tests need an unpacked distribution to run against.

## What it produces

Two flavours, built from one content spec: **slim** (`bin`, `server`, `docker`, `lib`, licenses) and **full** (slim plus `example`, `modules` and `cross-dc-manager`).

| Task | Output |
| --- | --- |
| `installFullDist` | unpacked full distribution at `build/solr-<version>` |
| `installSlimDist` | unpacked slim distribution at `build/solr-<version>-slim` |
| `installDist`, `assemble` | both of the above |
| `fullDistTar` / `slimDistTar` | `build/distributions/solr-<version>.tgz` and `-slim.tgz` |
| `distTar`, `assembleDist` | both tarballs |
| `devFull` | copy of the full distribution at `build/dev` |
| `devSlim` | copy of the slim distribution at `build/dev-slim` |
| `dev` | both `devFull` and `devSlim` |
| `runDev` | starts Solr from `build/dev-slim/server` with a plain `JavaExec`, bypassing `bin/solr` |

Only tarballs are produced; the `fullDistZip` and `slimDistZip` tasks that the `distribution` plugin creates are disabled. `assembleDist` (and `releaseArtifacts`) additionally require `-Psolr.ui.buildFromSource=true`, so that a release build never pulls the Admin UI from nightlies; `assemble`/`installDist` do not.

Why a developer cares: `bin/solr` resolves everything relative to an installed layout, so there is no way to exercise it from the source tree. You need an unpacked distribution. The `dev` tasks exist for working by hand, and they keep their data across runs:

```bash
./gradlew -p solr/packaging devSlim
cd solr/packaging/build/dev-slim && bin/solr start -f
```

The test tasks below use `build/solr-<version>` (via `installFullDist`), not `build/dev`, so a `dev` directory you are poking at by hand is never disturbed by a test run.

The `solrFullTgz` and `solrSlimTgz` artifact configurations published here are what `solr/distribution` consumes for signing and release assembly.

## Tests

Three layers, none of them part of `check`:

| Layer | Task(s) | Status |
| --- | --- | --- |
| Shell harness (JUnit, cross-platform) | `shellTests`, `shellTestsPicocli`, `shellTestsCommonsCli` | preferred, where new tests go |
| BATS suites (bash) | `integrationTests`, `integrationTestsPicocli` | deprecated, being ported to the harness |
| Stock `test` | — | disabled in this module |

The stock `test` task is turned off (`tasks.named('test') { enabled = false }`). Both real layers need an unpacked distribution and start real Solr nodes, which takes minutes, so they are deliberately kept out of the normal unit-test run and out of `check`. You have to ask for them by name.

### Shell harness (preferred)

JUnit suites in `src/test/org/apache/solr/shell/`, extending `SolrShellTestCase` from `solr/test-framework/src/java/org/apache/solr/shell/`. One test source runs against `bin/solr` under bash and `bin\solr.cmd` under `cmd.exe`: the harness picks `CMD` on Windows and `BASH` elsewhere (`Shell.defaultShell()`, overridable with `-Dsolr.cli.shell=BASH|CMD` or `SOLR_CLI_SHELL`), and a test writes one command string for both shells.

```bash
./gradlew -p solr/packaging shellTests
./gradlew -p solr/packaging shellTests --tests "*ZkTest"
```

A test reads much like its BATS original — `run("solr zk ls /").assertContains("aliases.json")` — with `${SOLR_PORT}`, `${ZK_PORT}`, `${SOLR_TIP}` and friends expanded from the session environment. `SolrShellTestCase` also provides the file, HTTP and wait helpers that the BATS suites got from shell tools (`touch`, `write`, `read`, `http`, `https`, `collectionExists`, `awaitOrFail`, ...), so a port does not need `curl`, `grep` or `find`.

Two session scopes mirror BATS: `classCli` is `setup_file` (start a long-lived node from `@BeforeClass`; it is stopped after the class), and `cli` is `setup` (environment, cwd and nodes are rolled back after each test).

| Task | CLI implementation |
| --- | --- |
| `shellTests` | whatever `bin/solr` defaults to (`SOLR_PICOCLI` unset) |
| `shellTestsPicocli` | picocli (`SOLR_PICOCLI=true`) |
| `shellTestsCommonsCli` | commons-cli (`SOLR_PICOCLI=false`); deprecated, goes away with commons-cli itself |

The two CLIs disagree on some usage and error wording. Where only the wording differs, assert both with `CommandResult.assertContainsForCli(commonsCliText, picocliText)`; where a test genuinely cannot hold for both, annotate it `@EnabledOnCli`. Use `@EnabledOnShell` for the rare test that is specific to one shell (a `solr.in.sh` versus `solr.in.cmd` detail, a Windows path quirk) — no annotation means "must pass on both", which is what a ported test should normally be.

Suites run in parallel forks, with each test *class* getting its own `SOLR_HOME`, `SOLR_PID_DIR`, logs directory and port block (`SOLR_PORT`, `+1`, `+2`, and ZooKeeper at `+1000`), claimed cross-fork by `PortBlocks`. That is what makes the parallelism safe: `SOLR_PID_DIR` scopes `solr stop --all`, and class setup wipes `SOLR_HOME`. Fork count defaults to `min(4, tests.jvms)` — capped because each fork may hold a Solr JVM plus embedded ZooKeeper — and is overridable:

```bash
./gradlew -p solr/packaging shellTests -Pshell.tests.jvms=1
```

The harness fails a class if a node or a port survives it, and kills nodes an earlier, hard-killed run left behind, by matching a marker on the node's command line. To run a suite from an IDE, build the distribution first and point the harness at it with `SOLR_TIP` (or `-Dsolr.cli.solr.tip=...`); every harness variable has a `solr.cli.<lowercase.dotted.name>` system-property form, and setting `SOLR_PORT` pins the port block instead of claiming a random one.

The harness library has its own unit tests, in `solr/test-framework`, under a task with the same name — so `./gradlew shellTests` from the repo root runs both. Those are part of `check`, since they need no distribution.

### BATS suites (deprecated)

`test/*.bats`, written for [BATS](https://bats-core.readthedocs.io/en/stable/index.html). **Deprecated**: they are bash-only, so they have never run on Windows, and they are slower and harder to maintain than JUnit. They are being ported to the shell harness one file at a time. Do not add tests here; see "Which do I write?" below.

```bash
./gradlew -p solr/packaging integrationTests
./gradlew -p solr/packaging integrationTests --tests test_zk.bats
```

Practical details for as long as the port is unfinished:

- **No manual install.** The `downloadBats` task fetches `bats-core` plus the `bats-support`, `bats-assert` and `bats-file` helpers (versions pinned in `gradle/libs.versions.toml`) into `build/bats-libs` over plain HTTP, and `integrationTests` depends on it. No npm, no node, nothing to install yourself.
- `--tests <file>.bats` selects a single file and may be repeated. Wildcards and individual test names are not supported.
- Tests run on a randomly assigned port, so they coexist with a Solr you have running. Pin it with `-Pbats.port=8983` (or the `bats.port` system property).
- Slow tests and tests needing network access are tagged `# bats test_tags=nightly` on the line before `@test`, and are filtered out with `--filter-tags !nightly` unless you pass `-Psolr.bats.nightly=true`.
- `integrationTestsPicocli` runs the same suites with `SOLR_PICOCLI=true`, against the experimental picocli CLI.
- Every test file should `load bats_helper` and call `common_setup` from its own `setup()`. Test names may contain only letters, digits and spaces — no dashes or underscores, so JIRA ids and option names are not valid names.
- Clusters and collections a test creates are **not** cleaned up for you. `test/test_bats.bats` is the worked example for setup, teardown, debugging and the subtler traps.
- Run `shellcheck` over anything you touch here.

CI runs `integrationTests` and `integrationTestsPicocli` on Linux from `.github/workflows/bin-solr-test.yml`, on pull requests that touch `solr/bin`, `solr/packaging` or `solr/core/src/java/org/apache/solr/cli`.

### Which do I write?

New coverage for `bin/solr`, `bin/solr.cmd` or anything in the CLI goes in the shell harness, as a JUnit suite under `src/test/org/apache/solr/shell/`. One test then covers both shells, which a `.bats` file can never do. Touch the `.bats` files only to keep them working — or to delete one once its port is in place. And as always: if the thing can be tested as an ordinary JUnit test without spawning a script, test it there instead of in either of these layers.

## Where the output lands

First thing to look at when something fails:

| | Shell harness | BATS |
| --- | --- | --- |
| Run output root | `build/shell-test-output` (`-picocli`, `-commons-cli` for the variants) | `build/test-output` (`build/test-output-picocli`) |
| Console log | per-test stdout/stderr under `build/test-results/<task>/outputs` | `test-output.txt` in the run output root, teed from the console |
| JUnit XML | `build/test-results/<task>` | the run output root |
| HTML report | `build/reports/tests/<task>`, only with `-Ptests.html=true` | — |
| `SOLR_HOME` | `<root>/<TestClass>/solr-home` | `<root>/solr-home` |
| Solr logs | `<SOLR_HOME>/logs` | `<root>/solr-home/logs` |
| Failure snapshot | `<root>/failure-snapshots/<TestClass>-<method>/`, holding a copy of `SOLR_HOME` and `transcript.log` (every command, with its output) | `<root>/failure-snapshots/` |

Both run output roots are wiped at the start of each run, so copy anything you want to keep. The harness prints the snapshot path on stderr for each failed test.

## See also

- `dev-docs/writing-tests.adoc` — how to write tests in Solr generally, and where each kind belongs.
- `solr/test-framework/README.md` — the test framework, including the `org.apache.solr.shell` harness library used here.
