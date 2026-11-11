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

# Findings from porting the BATS suites — DELETE THIS FILE BEFORE COMMITTING

> **This file is scratch, checked in only so reviewers of the POC PR can see what the port turned
> up. Remove it before the PR is committed**, once each entry has become a JIRA issue or has been
> judged not worth one. Nothing in the build or the docs references it.

Found by porting the BATS CLI suites to the `org.apache.solr.shell` JUnit harness, which runs one
test source against `bin/solr` under bash and `bin\solr.cmd` under cmd.exe.

Everything here is left **unfixed on purpose**. This PR fixes only `bin/solr.cmd` issues that the
harness itself cannot work around (so far: honouring `SOLR_PID_DIR`, which is what scopes
`solr stop --all` and therefore what lets Windows run test classes in parallel). Real Solr bugs and
pre-existing weaknesses in unrelated tests are recorded here instead — fixing them would make an
already large PR unreviewable, and each deserves its own issue and its own regression test.

Nothing here is a regression. These are long-standing issues that only became visible once the same
assertions ran on both shells, or that reading the old tests closely exposed.

Where a finding blocks a test, the port marks it `@EnabledOnShell` and says why at the site.

## 1. `solr.cmd` does not forward `--solr-home` to `run_example`

`bin/solr` appends the resolved home to the arguments the example tool receives:

```
solr/bin/solr:742   PASS_TO_RUN_EXAMPLE+=("--solr-home" "$SOLR_HOME")
```

`solr.cmd`'s `:set_solr_home_dir` sets `SOLR_HOME` but never adds it to `PASS_TO_RUN_EXAMPLE`
(invocation at `solr/bin/solr.cmd:813`). So on Windows `solr start -e cloud --solr-home X` silently
ignores `X` and writes the example into `%SOLR_TIP%\example\cloud` instead.

Impact beyond the tests: a user who points an example at their own directory gets their
distribution written into instead, and a read-only install directory would fail outright.

Affects: `ExampleTest` (class is `@EnabledOnShell(Shell.BASH)`, though reason 2 below is the
binding one).

## 2. `RunExampleTool` drops every `SOLR_*` environment variable on Windows

`RunExampleTool#startSolr`'s Windows branch builds the child node's environment while excluding
every variable whose name starts with `SOLR_` ("prune out env vars that may cause issues"). The
bash branch does not.

So on Windows a `solr start -e <example>` node ignores `SOLR_PID_DIR`, `SOLR_LOGS_DIR`, `SOLR_OPTS`
and the rest. For the harness that is fatal: the example's nodes inherit neither the per-class
`SOLR_PID_DIR` nor the `solr.process.scan.marker`, so they cannot be discovered, owned or stopped,
and the class teardown's leak check fails.

The pruning looks deliberate but is far broader than it needs to be — an allow-list, or excluding
only the few variables that genuinely confuse a child node, would restore parity.

Affects: `ExampleTest` (class-level `@EnabledOnShell(Shell.BASH)`).

## 3. `solr.cmd` rejects `--jvm-opts` for `stop` with different text than `bin/solr`

`bin/solr` parses start and stop options in one loop, so `solr stop --jvm-opts` reaches the
start-option validation and reports:

```
ERROR: JVM options are required when using the --jvm-opts option!
```

`solr.cmd` jumps straight to `:parse_stop_args` (`solr/bin/solr.cmd:393`), never sees the start
options, and falls through to `:invalid_cmd_line` (`solr/bin/solr.cmd:751`):

```
Invalid command-line option: --jvm-opts
```

Its own equivalent message exists but sits in the start path (`solr/bin/solr.cmd:958`) and is
unreachable for `stop`. Both shells do reject the argument promptly rather than hanging, which is
what the test is for, so this is a wording divergence rather than a functional bug — but there is no
common substring, which is why the test is a `BASH`/`CMD` pair.

Affects: `StartSolrTest.checkStopCommandDoesntHang` (split into two annotated tests).

## 4. `bin/solr start` returns once the port is listening, not once Solr answers

The start script polls for the port to accept a connection and then reports success; it never makes
an HTTP request. So `solr start` can succeed against a node no client can actually talk to — which
is why `test_ssl.bats` needed a `sleep 5` after starting a node whose TLS configuration made it
unreachable.

The `SslTest` port replaces that sleep with a real wait (`awaitOrFail` on the node appearing in
`live_nodes`), so the test no longer depends on the timing. The script behaviour is unchanged and
still papers over a node that starts but cannot serve.

Found while porting `test_ssl.bats`. Not blocking any test.

## 5. The mTLS negative assertions prove less than they appear to

`test_ssl.bats` asserts a client without a certificate is rejected by matching
`(bad_certificate|ClosedChannelException|Server refused connection)`. In practice Jetty surfaces a
server-side TLS rejection as `SolrServerException: Server refused connection`, which is also what a
plain connection failure produces — so the assertion would pass if the node were simply down.

`SslTest` keeps the assertion as the original had it, and both affected tests do make a successful
request first, which at least proves the node is up and reachable before the negative case runs.
Tightening it to assert *why* the handshake failed needs the TLS cause chain, which
`HttpsRequest` now exposes — worth doing, as its own change.

Found while porting `test_ssl.bats`.

## 6. `solr post` reports success when the target collection does not exist

`test_basic_auth.bats` posts to a collection it never creates:

```
solr post --name monitors ... monitor.xml
```

and asserts `1 files indexed.` — which the command does print. The post succeeds from the CLI's
point of view even though nothing was indexed anywhere, so the assertion is far weaker than it
reads.

The `BasicAuthTest` port keeps it verbatim rather than quietly changing what the original asserted.
Worth deciding separately whether `solr post` should fail, or at least warn, when the collection is
missing — and then giving it a test that says so.

Found while porting `test_basic_auth.bats`.

## 7. A duplicate `solr snapshot-create` exits 0 instead of erroring

`test_snapshots.bats` carries the comment:

```
# the way snapshots live, if you run create twice you get an error because snapshot already exists
```

Observed otherwise. In a run where an earlier test aborted before deleting its snapshot, the next
test created a snapshot of the same name on the same collection and the command exited 0.

Neither the BATS file nor the port asserts on this, so nothing fails either way — but the comment
and the behaviour disagree, and one of them is wrong. For whoever owns `SnapshotCreateTool`.

Found while porting `test_snapshots.bats`.

## 8. `solr postlogs` skips log records with an NPE message

Indexing Solr's own `solr.log`, the tool reports:

```
Error reading log record:43 from file:solr.log
Cannot invoke "String.split(String)" because "params" is null
```

for a handful of records (43-46 and 91 in one run), then carries on and exits 0. So a few records
are silently dropped and the message is an NPE rather than a diagnosis of the malformed line.

The log-record parser is reading a `params` field that is absent on some records. The BATS test
tolerated this because it only asserted partials, and the port does the same — the indexing does
succeed, which is what the test is about.

Found while porting `test_postlogs.bats`.

## 9. `solr start -e <example>` ignores the `SOLR_HOME` environment variable

The example tool decides where to put the example's home like this (`RunExampleTool:595-604`):

```java
if (solrHomeArg != null)            solrHomeDir = Path.of(solrHomeArg);          // --solr-home
else if (solr.home property set)    solrHomeDir = Path.of(solrHomeProp);
else if (cloud example)             solrHomeDir = exampleDir.resolve("cloud");   // $SOLR_TIP/example/cloud
else                                solrHomeDir = serverDir.resolve("solr");     // $SOLR_TIP/server/solr
```

Nothing in that chain sees the `SOLR_HOME` environment variable:

- `bin/solr` adds `--solr-home` to the example tool's arguments **only** in the `--solr-home`
  command-line branch (`solr/bin/solr:735-744`). The environment variable is not forwarded.
- `SOLR_HOME` maps to the system property `solr.solr.home`
  (`EnvToSyspropMappings.properties:15`), while the tool reads `solr.home`.

So `SOLR_HOME=/var/solr solr start -e techproducts` silently ignores `/var/solr` and writes the
example — cores, index data and, for a cloud example, `zoo_data` — into the **installation
directory**. On a packaged install that directory may well be read-only or root-owned.

**Observed**, not just inferred: a run of the ported `test_export.bats` suite left a `zoo_data`
directory in `solr/packaging/build/solr-11.0.0-SNAPSHOT/server/solr/`, which is not part of the
distribution and is not tracked in git.

This is not specific to the new harness. `configureBatsTask` sets `SOLR_HOME` as an environment
variable too (`solr/packaging/build.gradle:340`), so every BATS test using `solr start -e` has been
writing into the unpacked distribution that all the other tests then read — a shared, build-tree
directory, which is exactly what a test must not mutate.

Worth a JIRA on its own terms, independently of the tests: either the example tool should honour
`SOLR_HOME`, or `bin/solr` should forward it, or the tool should refuse to write into its own
installation directory.

Workaround used by the ports meanwhile: pass `--solr-home "${SOLR_HOME}"` explicitly
(`ExampleTest`, `ExportTest`), or avoid `-e` altogether and build the fixture with
`solr create` + `solr post` (`CompressionTest`, `SnapshotsTest`, `StreamTest`).

## 10. Pre-existing staleness in the test build, unrelated to the CLI

Noticed while fact-checking `dev-docs/writing-tests.adoc`. All three are harmless, but misleading.

- `gradle/testing/randomization.gradle:111` still registers a `tests.badapples` property,
  "Enables or disables @BadApple tests." The `@BadApple` annotation no longer exists in Lucene or
  Solr — the only remaining references in the tree are commented-out lines citing SOLR-12028.
- The build's failure summary hardcodes `:test` when it composes its reproduce line
  (`gradle/testing/failed-tests-at-end.gradle:33`), so a failure in `shellTests`, `integrationTests`
  or a `beast` run prints a command that names a task the test does not belong to.
- Most `dev-docs/*.adoc` files are partly or wholly Markdown despite the `.adoc` extension, so they
  do not render as AsciiDoc intends: 10 of the 12 carry Markdown headings or fenced code blocks, and
  four have no AsciiDoc syntax at all (`how-to-contribute.adoc`, `solr-source-code.adoc`,
  `IDEs.adoc`, `jvms.adoc`). The new `writing-tests.adoc` is AsciiDoc throughout and renders clean
  under `asciidoctor --failure-level=WARN`; converting the rest is its own cleanup.
