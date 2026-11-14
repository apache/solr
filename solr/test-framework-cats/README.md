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

# CATS — Cross-platform Automated Testing System

CATS is an independent framework for testing command-line programs through the platform's own shell. A test is Java; what it runs is a real shell process — `bash` on Unix, `cmd.exe` on Windows — so what the test exercises is the program as a user invokes it, quoting, `PATH` resolution and launcher script included.

The dependency set is deliberately thin: a JRE, JUnit 4, and `slf4j-api` for logging. Nothing else, at compile time or at run time.

## What it gives you

**One test source, both shells.** The same test class drives the program under `bash` and under `cmd.exe`. A command line is written once, in a single portable spelling, and the shell in use decides how it is quoted and invoked. Where the two genuinely differ, a test says so with `@EnabledOnShell("bash")` and is skipped elsewhere as an assumption failure rather than failing.

**Variable resolution before any shell sees the line.** `${NAME}`, `$NAME` and `{{NAME}}` all name an environment variable, and the harness substitutes them itself — in a command line and in an assertion's expected text alike, so the two cannot drift apart. An undefined name is an immediate `AssertionError` listing the names that are defined, instead of the empty string a shell would quietly splice in. `%NAME%` is rejected: a line written that way would pass on one shell and fail on the other.

**Rails that catch a one-shell test.** The portability tables below refuse `grep`, `|`, `&&` and their relatives in a command line. That is the opinionated part of CATS, and it is the product: a test that passes only on `bash` is exactly the failure mode the framework exists to catch, and the rejection names the replacement rather than just saying no.

**A transcript that reads as a session.** Every scope writes `transcript.log` as it goes, and a failure quotes it. The format is a pasteable shell session — the environment changes, the command, its exit status and duration, its output — and an HTTP request made through the helpers appears as the `curl` that reproduces it, so the record covers the whole session a test ran and not only its command-line half.

**Isolation per test class.** Each class gets its own work directory and, if the program listens on anything, its own disjoint block of ports, so classes run concurrently in separate forks. Processes a test starts carry a marker on their command line; a leak check before and after the class fails the run rather than leaving them behind.

### Unportable tools

A line whose command is one of these is rejected, because `cmd.exe` has no equivalent and the test would silently only work on one platform. The replacement is part of the message.

| instead of | use |
| --- | --- |
| `grep`, `egrep`, `fgrep` | `assertContains`, `assertMatches`, `linesMatching()` on the result |
| `awk`, `cut` | `fields(lineIndex)` |
| `sed` | `firstMatch(regex)`, or plain Java on `output()` |
| `wc`, `head`, `tail` | `lineCount()`, `firstLines(n)`, `lastLines(n)` |
| `sort`, `uniq`, `tr`, `xargs` | plain Java over `lines()` |
| `cat`, `ls`, `find` | `read(path)`, `list(dir)`, `list(dir, glob)` |
| `rm`, `cp`, `mv`, `mkdir`, `touch` | `rm`, `copy`, `Files.move`, `mkdirs`, `touch` |
| `curl`, `wget`, `jq` | `http(pathAndQuery)`, which needs no external tool |
| `sleep` | `awaitOrFail(what, timeout, interval, condition)` |
| `ps`, `pgrep`, `kill`, `pkill`, `lsof` | the harness tracks and stops what a test started |
| `pwd`, `basename`, `dirname`, `realpath` | `cwd()`, `path(p)` and `java.nio.file.Path` |
| `export`, `unset` | `env(name, value)`, `unset(name)` |
| `chmod`, `chown`, `ln`, `openssl`, `tar`, `unzip`, `which` | do it in Java, or mark the test `@EnabledOnShell("bash")` |

### Unportable operators

| rejected | why | instead |
| --- | --- | --- |
| `\|` | no `cmd.exe` equivalent | assert on the command's own output, or feed the next one with `runWithStdin()` |
| `&&`, `\|\|`, `;` | a run is one command | make it two `run()` calls, and assert the status in Java |
| `$(...)`, backticks | command substitution; double quotes do not protect it | compute the value in Java and concatenate |
| `'...'` | `cmd.exe` has no single-quote quoting, so the quotes would reach the command verbatim | double quotes, or `run(List.of(...))`, which quotes each argument for the shell in use |
| `/dev/null`, `/tmp/` | quoting does not make a Unix path exist on Windows | a `workDir()`-relative path |
| a leading `NAME=value` | `bash`-only syntax | `env("NAME", value)` before the `run()` |

Operators are checked outside quoted spans, so a URL carrying `&` inside double quotes is fine; command substitution and Unix paths are checked across the whole line, because quoting does not neutralise those. A test that has already declared itself one shell's may call `cli().allowShellSpecific()` and use that shell freely — `@EnabledOnShell` sets it automatically.

## A worked example: a tool with no ports

A program that reads its arguments, prints, writes files and exits needs no configuration at all. `CatsTestCase` is the base class for a project with no test base class of its own, and the program it drives is a `ProgramUnderTest` with nothing overridden.

```java
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.apache.solr.cats.CatsTestCase;

public class MytoolConvertTest extends CatsTestCase {

  @Test
  public void convertsOneFile() {
    write("in/data.csv", "id,value\nROW1,0.47\n");
    mkdirs("out");

    run("mytool convert --in in/data.csv --out out/data.json")
        .assertExitCode(0)
        .assertContains("1 record");

    assertTrue(exists("out/data.json"));
    assertTrue(fileContains("out/data.json", "ROW1"));
  }

  @Test
  public void rejectsAMissingInput() {
    runFail("mytool convert --in nope.csv --out out/x.json").assertContains("no such file");
  }

  @Test
  public void readsItsInputFromStandardInput() {
    runWithStdin("id,value\nROW1,0.47\n", "mytool convert --in - --out out/stdin.json")
        .assertExitCode(0);
  }
}
```

`run(line)` asserts exit 0 and returns a `CommandResult`; `runFail(line)` asserts a non-zero one; `tryRun(line)` asserts nothing. `run(List.of(...))` takes an already-split argv and quotes each element for the shell. Overloads take a per-command `Duration`. Paths are relative to the test's own work directory, which is wiped per class, and `${NAME}` in any of them resolves against the session's environment.

The vocabulary — `run`, `runFail`, `env`, `cd`, `write`, `read`, `list`, `http`, `awaitOrFail`, `ports` and the rest — lives on the `CatsDsl` interface as default methods over a single `cli()`. `CatsTestCase` implements it. A project whose tests already extend a base class of its own implements `CatsDsl` on that class instead and wires a `CatsSessionRule`; nothing in the vocabulary is inherited twice.

Where the installation under test lives is `CATS_INSTALL_DIR`; its `bin` directory goes on `PATH`, which is how one spelling of `mytool` resolves to `bin/mytool` on one platform and `bin\mytool.cmd` on the other — `PATHEXT` does that, not a rewrite. A tool already on `PATH` needs no installation directory at all.

## What a server adds

Ports, discovery and stopping belong to the program, not to the harness, and a long-running program says so by overriding on `ProgramUnderTest`:

```java
public class MytoolServer implements ProgramUnderTest {

  /** Two ports per test class: the API and the admin endpoint. Stride 4 keeps blocks disjoint. */
  @Override
  public PortLayout portLayout() {
    return new PortLayout(4, new int[] {0, 1}, 20_000, 30_000, 200, 0);
  }

  /** The instances whose command line carries this run's marker, found however the platform allows. */
  @Override
  public List<RunningProgram> running(String marker) {
    return ProcessHandle.allProcesses()
        .filter(p -> p.info().commandLine().orElse("").contains(marker))
        .map(p -> new RunningProgram(p.pid(), portOf(p)))
        .toList();
  }

  /** Tried before the harness resorts to killing the process. */
  @Override
  public Optional<String> stopCommand(int port) {
    return Optional.of("mytool stop --port " + port);
  }

  /** Removed after a force-kill, so a later command does not walk them. */
  @Override
  public List<Path> staleStateFiles(Path stateDir, int port) {
    return List.of(stateDir.resolve("mytool-" + port + ".pid"));
  }

  /** The named traits of this run, read off the environment. */
  @Override
  public Map<String, String> traits(Map<String, String> env) {
    return Map.of("storage", env.getOrDefault("MYTOOL_STORAGE", "memory"));
  }
}
```

`PortLayout` is the shape of a block — the stride between blocks, the offsets within one, the range of bases a run may start from, how many blocks it may hand out, and which offset is *primary*, the one a port-less `http(path)` goes to. `PortLayout.NONE` is the default, and a program with no primary port gets a clear failure from `http(path)` rather than a silent guess. `PortBlock.at(i)` addresses the ports; naming their roles is the project's job, in a small view over the block, so that a test reads `ports().api()` rather than `ports().at(0)`.

The variables the program reads are named in one place, a `CatsHarnessEnv` subclass:

```java
public class MytoolEnv extends CatsHarnessEnv {

  public MytoolEnv(Class<?> testClass) {
    super(new MytoolServer(), testClass.getSimpleName(), null, null);
  }

  @Override
  protected void assemble(
      Map<String, String> vars,
      Optional<Path> installDir,
      Path classDir,
      PortBlock ports,
      String marker) {
    installDir.ifPresent(dir -> vars.put("MYTOOL_HOME", dir.toString()));
    vars.put("MYTOOL_DATA", createDir(classDir.resolve("data"), "the data dir").toString());
    vars.put("MYTOOL_PORT", String.valueOf(ports.at(0)));
    vars.put("MYTOOL_ADMIN_PORT", String.valueOf(ports.at(1)));
    vars.put("MYTOOL_OPTS", "-Dmytool.marker=" + marker);
  }

  /** Which names an "unknown variable ${TYPO}" message lists; empty means all of them. */
  @Override
  public Set<String> echoedPrefixes() {
    return Set.of("MYTOOL_");
  }
}
```

A subclass may also override `installDirVariable()`, `requiresInstallDir()` and `noInstallDirMessage()` to make an installation mandatory with its own advice on how to get one, `checkInstallDir(dir)` to validate that a directory really is one, `lookupPrefix()` for the system-property prefix its own variables are read under, and `workDir()` / `stateDir()` / `logDir()` to report back the directories it created — those are what the per-class wipe and the failure snapshot work on.

The two halves meet in `CatsSessionRule`, which resolves the environment per class, wipes the work directory, runs the leak checks, opens a class-scoped session and a child session per test, and snapshots the named directories plus `transcript.log` when a test fails. `CatsTestCase` is itself nothing but that wiring, and is worth reading as the template for a project's own base class.

## Run settings

The harness' own settings are read from the environment first, then from the system property the name maps to — `CATS_OUTPUT_DIR` is also `-Dcats.output.dir`. The environment is how a build task supplies a value; the property is how a developer running one class from an IDE overrides it.

| setting | default | what it does |
| --- | --- | --- |
| `CATS_SHELL` / `cats.shell` | `cmd` on Windows, `bash` elsewhere | the shell id every command goes through |
| `CATS_TIMEOUT` / `cats.timeout` | 300 (seconds) | the run-wide default a command is given to finish in |
| `CATS_PORT_BASE` / `cats.port.base` | random per run | where this run's port blocks start, so a failing run can be replayed on the same ports |
| `CATS_OUTPUT_DIR` / `cats.output.dir` | a temporary directory | where every scope writes: its work directory, its transcript, the failure snapshots |
| `CATS_INSTALL_DIR` / `cats.install.dir` | none | the installation under test, when the program is not already on `PATH` |
| `CATS_MARKER` / `cats.marker` | derived per class | the command-line marker this run's processes carry |

The timeout has three levels, narrowest first: the per-call overload `run(Duration, line)`, the session's own `cli().timeout(duration)`, and this run-wide default.

**Nothing harness-named is exported into the program's environment.** CATS has no port, no home and no settings of its own that the program could want, so there is no `CATS_PORT` and no `CATS_HOME`: every variable the program sees is one its own `assemble` put there, under the name the program knows it by. That separation is what keeps a program's configuration honest — the test exercises the same variables a user sets.

## Traits, and writing your own gate

A run has named traits, reported by the program off its environment. Which shell is in use is the one the framework knows, and `@EnabledOnShell` gates on it. Everything else is a name/value pair, and CATS ships no annotation for it on purpose: a typed, well-named annotation belongs with whoever knows the trait.

Two primitives are all it takes. `Traits.assume(traits, name, allowed...)` skips as an assumption failure unless the run has one of the given values, and `Traits.wordingFor(traits, name, byValue)` picks the expected text a trait's value calls for, which keeps one test covering every value where only a word of output differs. Both are reachable from a test as `traits()` and `wordingFor(name, byValue)`.

An annotation and a ten-line rule gate on a trait of your own:

```java
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EnabledOnStorage {
  String[] value();
}

public final class StorageGate implements TestRule {

  private final Supplier<Map<String, String>> traits;

  public StorageGate(Supplier<Map<String, String>> traits) {
    this.traits = traits;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        EnabledOnStorage on = description.getAnnotation(EnabledOnStorage.class);
        if (on != null) {
          Traits.assume(traits.get(), "storage", on.value());
        }
        base.evaluate();
      }
    };
  }
}
```

Wire it alongside the built-in `ShellGate` in the project's base class. A gate that must run before anything is started — so that a skipped class costs nothing — goes through `CatsSessionRule.openClass(testClass, gates)`, which calls it as soon as the environment is resolved and before the work directory is wiped or a port block claimed.

## Adding a shell

`Shells.BASH` and `Shells.CMD` ship; `Shells.current()` picks one and `Shells.byId(id)` looks one up. A shell is an interface, so adding one is implementing four methods and registering it, typically from a static initialiser in the project's test setup:

```java
public final class PowerShell implements Shell {

  @Override
  public String id() {
    return "powershell";
  }

  /** How the line becomes an argv for ProcessBuilder, given a scratch dir to write a script in. */
  @Override
  public List<String> processArgs(String line, Path scriptDir, String scriptName) {
    return List.of("pwsh", "-NoProfile", "-Command", line);
  }

  /** How one argument is quoted so this shell passes it through intact. */
  @Override
  public String quote(String arg) {
    return "'" + arg.replace("'", "''") + "'";
  }
}

Shells.register(new PowerShell());
```

`quoteForDisplay(arg)` and `joinArgs(argv)` have defaults worth overriding only if the display quoting is misleading for that shell. `@EnabledOnShell` takes ids, so a test for the new shell says `@EnabledOnShell("powershell")`, and `CATS_SHELL=powershell` runs the suite through it. The quoting rules are the whole job, and they belong with the project that needs that shell.

## Running the module's own tests

The harness' own unit tests need nothing installed and run in seconds, so they are the stock `test` task and part of `check`:

```
./gradlew -p solr/test-framework-cats test
./gradlew -p solr/test-framework-cats catsTests
```

`catsTests` is an alias for `test` here, so that one task name run from the repository root covers the harness' tests and the command-line suites that use it alike. They drive shell built-ins and a small fake program, and exercise exactly what a user of the framework depends on: the portability rails and their messages, variable resolution, quoting on both shells, port-block claiming, the transcript, process cleanup, and the zero-configuration shape in which a program that declares nothing still gets a shell, a session, a work directory and the whole vocabulary.
