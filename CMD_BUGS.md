# `bin/solr.cmd` bugs and bash/cmd divergences found while porting the BATS suites

Found by porting the BATS CLI suites to the `org.apache.solr.shell` JUnit harness, which runs one
test source against `bin/solr` under bash and `bin\solr.cmd` under cmd.exe. Each entry is left
unfixed on purpose: the port marks the affected test `@EnabledOnShell` and the fix belongs in its
own JIRA.

Nothing here is a regression — these are long-standing differences that only became visible once
the same assertions ran on both shells.

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
