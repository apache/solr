/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cats.session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.solr.cats.Traits;
import org.apache.solr.cats.env.CatsHarnessEnv;
import org.apache.solr.cats.env.PortBlock;
import org.apache.solr.cats.exec.CatsFiles;
import org.apache.solr.cats.exec.CommandResult;
import org.apache.solr.cats.exec.ProcessKill;
import org.apache.solr.cats.exec.ProgramRegistry;
import org.apache.solr.cats.exec.ProgramUnderTest;
import org.apache.solr.cats.exec.RunningProgram;
import org.apache.solr.cats.exec.Shell;
import org.apache.solr.cats.exec.ShellAssertionError;
import org.apache.solr.cats.exec.ShellExecutor;
import org.apache.solr.cats.exec.Shells;
import org.apache.solr.cats.exec.Variables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shell session: the environment, working directory, shell and timeout that every command in a
 * scope runs with, plus the state needed to clean up afterwards.
 *
 * <p>There is no long-lived shell process. Every {@link #run} starts a fresh one with this
 * session's environment and cwd, so quoting, globbing and variable expansion are done by the real
 * shell while control flow stays in Java. One session is scoped to a test class; each test gets a
 * {@linkplain #child child} of it whose environment and cwd changes are discarded afterwards.
 *
 * <p>Sessions track which nodes they started by scanning the running process list after each
 * command, filtered to this scope's marker and port block, and stop exactly those on {@link
 * #close()}.
 */
public class CliSession implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final AtomicInteger SEQ = new AtomicInteger();

  private final CatsHarnessEnv harnessEnv;
  private final ProgramUnderTest program;
  private final ProgramRegistry registry;
  private final CliSession parent;
  private final String name;
  private final Path workDir;
  private final Transcript transcript;
  private final CatsCliHelpers helpers;
  private final CatsNet net;
  private final Map<String, String> vars;
  private final Shell shell;

  private final List<Process> startedProcesses = new ArrayList<>();
  private final Map<Integer, Long> ownedNodes = new LinkedHashMap<>();

  private Path cwd;
  private Duration timeout;
  private CommandResult lastResult;
  private boolean closed;
  private boolean shellSpecific;

  private CliSession(
      CatsHarnessEnv harnessEnv,
      ProgramUnderTest program,
      CliSession parent,
      String name,
      Path workDir,
      Map<String, String> vars,
      Path cwd,
      Shell shell) {
    this.harnessEnv = harnessEnv;
    this.program = program;
    this.registry = harnessEnv.registry();
    this.parent = parent;
    this.name = name;
    this.workDir = workDir;
    this.vars = vars;
    this.cwd = cwd;
    this.shell = shell;
    this.timeout = harnessEnv.timeout();
    this.transcript = new Transcript(workDir.resolve("transcript.log"));
    this.helpers = new CatsCliHelpers(harnessEnv, shell, transcript, varsForAssertions());
    this.net = new CatsNet(transcript, this::path);
    // Exposed as a variable so a test can name its own scratch space inside a command line or an
    // assertion — ${WORK_DIR}/something — instead of formatting a Java path into the string.
    this.vars.put("WORK_DIR", workDir.toString());
  }

  /** Creates the class-scoped session. Its work dir is {@code <output dir>/work/<name>}. */
  public static CliSession forScope(CatsHarnessEnv env, ProgramUnderTest program, String name) {
    Path workDir = env.outputDir().resolve("work").resolve(sanitize(name));
    createDir(workDir);
    Map<String, String> vars = newEnvMap();
    vars.putAll(env.baseEnv());
    // Starts in its own work directory, so a relative path in a command means something per
    // scope and a test needs no cd() to create or name a scratch file.
    return new CliSession(env, program, null, name, workDir, vars, workDir, Shells.current());
  }

  /**
   * Creates a child session for one test. It starts with a copy of this session's environment and
   * cwd, so nothing a test changes leaks into the next one.
   */
  public CliSession child(String childName) {
    Path childWorkDir = workDir.resolve(sanitize(childName));
    createDir(childWorkDir);
    Map<String, String> childVars = newEnvMap();
    childVars.putAll(vars);
    // The child gets its own work directory as its cwd, unless the parent had cd'd elsewhere.
    Path childCwd = cwd.equals(workDir) ? childWorkDir : cwd;
    CliSession child =
        new CliSession(
            harnessEnv, program, this, childName, childWorkDir, childVars, childCwd, shell);
    child.timeout = timeout;
    child.shellSpecific = shellSpecific;
    return child;
  }

  // ---------------------------------------------------------------- state

  /** Sets an environment variable for subsequent commands in this session. */
  public CliSession env(String key, String value) {
    vars.put(key, value);
    return this;
  }

  /**
   * Removes an environment variable for subsequent commands, as a shell's {@code unset} does.
   *
   * <p>Needed because some behaviour of a program is only reachable with a variable absent rather
   * than empty: a launcher may check whether a variable is set at all before falling back to its
   * default.
   */
  public CliSession unset(String key) {
    vars.remove(key);
    return this;
  }

  /** The current value of an environment variable, or null. */
  public String env(String key) {
    return vars.get(key);
  }

  /**
   * An unmodifiable view of this session's variables, for the assertion families that resolve
   * {@code ${NAME}} in their expected text.
   */
  public Map<String, String> varsForAssertions() {
    return Collections.unmodifiableMap(vars);
  }

  /** Changes the working directory. A relative path resolves against the current one. */
  public CliSession cd(String dir) {
    Path target = cwd.resolve(dir).normalize();
    if (!Files.isDirectory(target)) {
      throw new AssertionError("cd: not a directory: " + target);
    }
    cwd = target;
    return this;
  }

  public CliSession cd(Path dir) {
    return cd(dir.toString());
  }

  /**
   * Creates an empty file, as {@code touch} does, resolving a relative path against {@link #cwd()}
   * and creating any missing parent directories.
   *
   * <p>In Java rather than by running {@code touch}, which cmd.exe does not have. An existing file
   * is left as it is — tests use this to make a file exist, not to bump a timestamp.
   *
   * @return the absolute path, so a test can assert on it or pass it to a command
   */
  public Path touch(String path) {
    Path file = cwd.resolve(path).normalize();
    try {
      Files.createDirectories(file.getParent());
      if (!Files.exists(file)) {
        Files.createFile(file);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Could not create " + file, e);
    }
    return file;
  }

  // ------------------------------------------------------------ file work, in Java
  // The shell file tools a test reaches for — mkdir, rm, cp, cat, ls, find, wc — exist on
  // bash and not on cmd.exe, so the harness offers them instead of letting a line hardcode one
  // shell. Every path is resolved against {@link #cwd()}, which is the scope's own work directory,
  // so a relative path behaves exactly as it would in the shell.

  /** Resolves a path the way the shell would: relative to the session's working directory. */
  public Path path(String path) {
    return cwd.resolve(path).normalize();
  }

  /** Port of {@code mkdir -p}. */
  public Path mkdirs(String path) {
    Path dir = path(path);
    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not create directory " + dir, e);
    }
    return dir;
  }

  /** Port of {@code echo <content> > <path>}, creating parent directories as needed. */
  public Path write(String path, String content) {
    Path file = path(path);
    try {
      Files.createDirectories(file.getParent());
      Files.writeString(file, content, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not write " + file, e);
    }
    return file;
  }

  /** Port of {@code echo <content> >> <path>}. */
  public Path append(String path, String content) {
    Path file = path(path);
    try {
      Files.createDirectories(file.getParent());
      Files.writeString(
          file,
          content,
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not append to " + file, e);
    }
    return file;
  }

  /** Port of {@code cat}. */
  public String read(String path) {
    Path file = path(path);
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not read " + file, e);
    }
  }

  /** Port of {@code cat <path> | grep <regex>}: the file's lines the pattern matches. */
  public List<String> readLinesMatching(String path, String regex) {
    // Resolved, like fileContains' argument: the two are siblings and must not differ in whether
    // a ${NAME} in the pattern means anything.
    Pattern pattern =
        Pattern.compile(
            Variables.resolve(regex, Variables.ANY, vars, "a file assertion", echoedPrefixes()));
    return read(path).lines().filter(line -> pattern.matcher(line).find()).toList();
  }

  /** Port of {@code grep -q <text> <path>}. */
  public boolean fileContains(String path, String text) {
    return read(path)
        .contains(
            Variables.resolve(text, Variables.ANY, vars, "a file assertion", echoedPrefixes()));
  }

  /** Port of {@code [ -e <path> ]}. */
  public boolean exists(String path) {
    return Files.exists(path(path));
  }

  /** Port of {@code rm -rf}. Removing something that is not there is not an error. */
  public void rm(String path) {
    Path target = path(path);
    try {
      if (Files.isDirectory(target)) {
        CatsFiles.rm(target);
      } else {
        Files.deleteIfExists(target);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Could not delete " + target, e);
    }
  }

  /** Port of {@code cp} and {@code cp -r}: copies a file, or a directory tree, recursively. */
  public Path copy(String from, String to) {
    Path source = path(from);
    Path target = path(to);
    try {
      if (Files.isDirectory(source)) {
        try (Stream<Path> tree = Files.walk(source)) {
          for (Path entry : tree.toList()) {
            Path destination = target.resolve(source.relativize(entry).toString());
            if (Files.isDirectory(entry)) {
              Files.createDirectories(destination);
            } else {
              Files.createDirectories(destination.getParent());
              Files.copy(entry, destination, StandardCopyOption.REPLACE_EXISTING);
            }
          }
        }
      } else {
        Files.createDirectories(target.getParent());
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Could not copy " + source + " to " + target, e);
    }
    return target;
  }

  /**
   * Port of {@code ls <glob>} and of {@code find <dir> -name <glob>}: the matching paths, sorted.
   *
   * <p>{@code glob} is a {@link java.nio.file.FileSystem#getPathMatcher glob} pattern relative to
   * {@code dir}, so {@code "*.txt"} lists one level and <code>"**&#47;*.xml"</code> walks the tree.
   */
  public List<Path> list(String dir, String glob) {
    Path base = path(dir);
    if (!Files.isDirectory(base)) {
      return List.of();
    }
    PathMatcher matcher = base.getFileSystem().getPathMatcher("glob:" + glob);
    try (Stream<Path> tree = Files.walk(base)) {
      return tree.filter(entry -> !entry.equals(base))
          .filter(entry -> matcher.matches(base.relativize(entry)))
          .sorted()
          .toList();
    } catch (IOException e) {
      throw new UncheckedIOException("Could not list " + base, e);
    }
  }

  /** Port of {@code ls <dir>}: the names directly in a directory, sorted. */
  public List<Path> list(String dir) {
    return list(dir, "*");
  }

  // ------------------------------------------------------------ a local web server

  /** Serves a directory over HTTP on a free port, and returns the port. */
  public int serveHttp(String webRoot) {
    requireOpen();
    return net.serveHttp(webRoot);
  }

  /** Stops one server started by {@link #serveHttp}. Stopping one twice is not an error. */
  public void serveHttpShutdown(int port) {
    net.serveHttpShutdown(port);
  }

  /** Fetches a URL into a file, and returns the absolute path of that file. */
  public Path download(String url, String path) {
    return net.download(url, path);
  }

  public Path cwd() {
    return cwd;
  }

  /** Scratch space for this scope: temp scripts, captured output, the transcript. */
  public Path workDir() {
    return workDir;
  }

  public Shell shell() {
    return shell;
  }

  public PortBlock ports() {
    return harnessEnv.ports();
  }

  public CatsHarnessEnv harnessEnv() {
    return harnessEnv;
  }

  /** The named traits of this session's run, as the program reads them off its environment. */
  public Map<String, String> traits() {
    return program.traits(vars);
  }

  /** The wording trait {@code name}'s value calls for in this run. */
  public String wordingFor(String name, Map<String, String> byValue) {
    return Traits.wordingFor(traits(), name, byValue);
  }

  /** The variable-name prefixes an unresolved {@code ${NAME}} lists as defined. */
  public Set<String> echoedPrefixes() {
    return harnessEnv.echoedPrefixes();
  }

  public Transcript transcript() {
    return transcript;
  }

  /** The conveniences for this session that are not about running a command: HTTP, log tails. */
  public CatsCliHelpers helpers() {
    return helpers;
  }

  /**
   * Turns off the cross-shell portability rails for this session.
   *
   * <p>For a test that has already declared itself one shell's with {@link
   * org.apache.solr.cats.EnabledOnShell} — set automatically there. Such a test may use that
   * shell's pipes, quoting and tools freely, since there is no other shell for it to break on.
   */
  public CliSession allowShellSpecific() {
    this.shellSpecific = true;
    return this;
  }

  /** Overrides the default per-command timeout for this session. */
  public CliSession timeout(Duration newTimeout) {
    this.timeout = newTimeout;
    return this;
  }

  /** The result of the most recent command in this session. */
  public CommandResult lastResult() {
    return lastResult;
  }

  // ---------------------------------------------------------------- exec

  /**
   * Runs a command line.
   *
   * <p>Variables are written the way a shell test writes them, so one line works on every shell:
   *
   * <pre>
   *   run("mytool list --server localhost:${REGISTRY_PORT}");
   *   run("mytool start -p $SECOND_PORT");
   * </pre>
   *
   * <p>{@code ${NAME}}, {@code $NAME} and {@code {{NAME}}} resolve against the session environment,
   * substituted by the <em>harness</em> rather than the shell — which is what makes one line work
   * on both, since bash expands {@code $NAME} and cmd.exe wants {@code %NAME%}. Resolving in Java
   * also fails the test on an unknown name instead of quietly substituting the empty string, and
   * lets the transcript record the real command. Reading {@code $NAME} as an environment variable
   * is sound because a {@code run} line has no shell state of its own: one process per command, so
   * no locals or positional parameters. {@code %NAME%} is rejected rather than substituted, {@code
   * %} being {@link String#format}'s; {@code $'..'}, {@code $1} and a bare {@code $} are left for
   * the shell.
   *
   * <p>A Java value is simply concatenated — {@code run("mytool start -p " + myPort)}. Sharp edge:
   * that happens before the harness resolves the line, so a value containing {@code ${...}} is
   * taken as a variable reference. An argument needing per-shell quoting goes through {@link
   * #run(List)}.
   */
  public CommandResult run(String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    String resolved = resolveForRun(line);
    return requireExit(execute(resolved, timeout, false), true);
  }

  /**
   * Like {@link #run(String)} but for a command that is expected to fail, so that the test can
   * assert on what it printed. Fails if the command unexpectedly succeeds.
   */
  public CommandResult runFail(String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    String resolved = resolveForRun(line);
    return requireExit(execute(resolved, timeout, false), false);
  }

  /**
   * Runs a command without asserting anything about its status, for a predicate that inspects the
   * result itself - a poll that expects failure until some state changes, say. Prefer {@link
   * #run(String)} or {@link #runFail(String)}, which state the expectation.
   */
  public CommandResult tryRun(String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    String resolved = resolveForRun(line);
    return execute(resolved, timeout, false);
  }

  /** The status check behind {@code run} and {@code runFail}; neither lets one pass silently. */
  private static CommandResult requireExit(CommandResult result, boolean expectSuccess) {
    if (expectSuccess == result.failed()) {
      throw result.fail(
          expectSuccess
              ? "command exited " + result.status() + ", expected 0"
              : "command exited 0, expected a non-zero status");
    }
    return result;
  }

  /** Runs a command line with an explicit timeout. */
  public CommandResult run(Duration commandTimeout, String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    return requireExit(execute(resolveForRun(line), commandTimeout, false), true);
  }

  /** Like {@link #run(Duration, String)} but for a command that is expected to fail. */
  public CommandResult runFail(Duration commandTimeout, String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    return requireExit(execute(resolveForRun(line), commandTimeout, false), false);
  }

  /**
   * Runs an argv, letting the harness quote each argument for the current shell. Each argument has
   * its variables resolved before it is quoted.
   */
  public CommandResult run(List<String> argv) {
    // Only the tool name: an argv element may contain anything, since the harness quotes it.
    if (!shellSpecific) {
      requirePortableTool(argv.isEmpty() ? "" : argv.get(0));
    }
    String line = shell.joinArgs(argv.stream().map(this::resolveForRun).toList());
    return requireExit(execute(line, timeout, false), true);
  }

  /** Like {@link #run(List)} but for a command that is expected to fail. */
  public CommandResult runFail(List<String> argv) {
    if (!shellSpecific) {
      requirePortableTool(argv.isEmpty() ? "" : argv.get(0));
    }
    String line = shell.joinArgs(argv.stream().map(this::resolveForRun).toList());
    return requireExit(execute(line, timeout, false), false);
  }

  /** Like {@link #run(String)} but keeps {@code stdout()} and {@code stderr()} apart. */
  public CommandResult runSeparated(String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    String resolved = resolveForRun(line);
    return requireExit(execute(resolved, timeout, true), true);
  }

  /**
   * Like {@link #run(String)} but feeds {@code stdin} to the command on its standard input.
   *
   * <p>The text goes in through the process itself rather than through a shell pipe, so one line
   * works on every shell and nothing in it has to be quoted or escaped. The command line is still
   * held to the cross-shell rails.
   */
  public CommandResult runWithStdin(String stdin, String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    return requireExit(execute(resolveForRun(line), timeout, false, stdin), true);
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Session '" + name + "' is already closed");
    }
  }

  /**
   * A run is one command. A newline in the line would change what cmd.exe does without saying so:
   * Shells.CMD drops the {@code call} for a multi-line body, and without {@code call} a batch file
   * that invokes another batch file never gets control back. Easy to write by accident in a text
   * block.
   *
   * <p>Only the string form is checked. An argv element may legitimately contain a newline — the
   * shell quoting takes care of it — which is how a line like {@code $'id,value\nROW1,0.47'} is
   * written.
   */
  private void requireSingleCommand(String line) {
    if (line.strip().contains("\n")) {
      throw new AssertionError(
          "A run() line must be a single command; make it one run() call each: " + line);
    }
  }

  /**
   * The Unix-only tools a shell line reaches for, each with the harness feature that replaces it.
   *
   * <p>None of these exist on cmd.exe, so a line invoking one is a test that silently only works on
   * one platform — caught here rather than on Windows three weeks later, and with the replacement
   * named.
   */
  private static final Map<String, String> UNIX_ONLY_TOOLS =
      Map.ofEntries(
          Map.entry("grep", "assert on the output: assertContains/assertMatches/linesMatching()"),
          Map.entry("egrep", "assertMatches(), or linesMatching()"),
          Map.entry("fgrep", "assertContains(), or linesMatching()"),
          Map.entry("awk", "fields(lineIndex) on the result"),
          Map.entry("cut", "fields(lineIndex) on the result"),
          Map.entry("sed", "firstMatch(regex) on the result, or plain Java on output()"),
          Map.entry("tr", "plain Java on output()"),
          Map.entry("wc", "lineCount() on the result"),
          Map.entry("head", "firstLines(n) on the result"),
          Map.entry("tail", "lastLines(n) on the result"),
          Map.entry("sort", "sort the result's lines() in Java"),
          Map.entry("uniq", "de-duplicate the result's lines() in Java"),
          Map.entry("cat", "read(path)"),
          Map.entry("ls", "list(dir) or list(dir, glob)"),
          Map.entry("find", "list(dir, glob)"),
          Map.entry("rm", "rm(path)"),
          Map.entry("rmdir", "rm(path)"),
          Map.entry("cp", "copy(from, to)"),
          Map.entry("mv", "Files.move(path(from), path(to))"),
          Map.entry("mkdir", "mkdirs(path)"),
          Map.entry("touch", "touch(path)"),
          Map.entry("diff", "compare read(a) and read(b) in Java"),
          Map.entry("curl", "http(pathAndQuery), which needs no external tool"),
          Map.entry("wget", "http(pathAndQuery)"),
          Map.entry("jq", "assert on http(..).body(), or parse it in Java"),
          Map.entry("sleep", "awaitOrFail(what, timeout, interval, condition)"),
          Map.entry(
              "ps", "the harness tracks what a test started; use the test case's status helpers"),
          Map.entry(
              "kill", "the harness stops what a test started; use the test case's stop helpers"),
          Map.entry(
              "pkill", "the harness stops what a test started; use the test case's stop helpers"),
          Map.entry("pgrep", "the harness tracks what a test started"),
          Map.entry("lsof", "the harness owns its port block; no process lookup needed"),
          Map.entry("chmod", "no cmd.exe equivalent — mark the test @EnabledOnShell(\"bash\")"),
          Map.entry("chown", "no cmd.exe equivalent — mark the test @EnabledOnShell(\"bash\")"),
          Map.entry("ln", "Files.createSymbolicLink, and @EnabledOnShell(\"bash\")"),
          Map.entry("openssl", "no cmd.exe equivalent — mark the test @EnabledOnShell(\"bash\")"),
          Map.entry("tar", "unpack it in Java, or mark the test @EnabledOnShell(\"bash\")"),
          Map.entry("unzip", "unpack it in Java, or mark the test @EnabledOnShell(\"bash\")"),
          Map.entry("xargs", "loop in Java"),
          Map.entry("tee", "the harness already captures the output"),
          Map.entry("which", "no cmd.exe equivalent"),
          Map.entry("basename", "path(p).getFileName()"),
          Map.entry("dirname", "path(p).getParent()"),
          Map.entry("realpath", "path(p), which normalizes"),
          Map.entry("pwd", "cwd()"),
          Map.entry("export", "env(name, value)"),
          Map.entry("unset", "unset(name)"),
          Map.entry("source", "env(name, value) for what it would set, or @EnabledOnShell"),
          Map.entry("eval", "build the line in Java, or @EnabledOnShell"));

  /**
   * Shell operators with no cmd.exe equivalent. Double quotes neutralise all of these, so they are
   * only a problem outside a quoted span — a URL carrying {@code &} in quotes is fine.
   */
  private static final List<String[]> UNPORTABLE_OPERATORS =
      List.of(
          new String[] {
            "|", "a pipe", "assert on the command's own output, or feed it with runWithStdin()"
          },
          new String[] {"&&", "a chained command", "run() is one command; make it two run() calls"},
          new String[] {
            "||", "a chained command", "run() is one command; assert the status in Java instead"
          },
          new String[] {";", "a command sequence", "make it two run() calls"});

  /**
   * bash constructs that double quotes do <em>not</em> protect, so they are a problem anywhere in
   * the line.
   *
   * <p>Command substitution is the one that catches people out: {@code "$(date)"} looks quoted and
   * safe, but bash still runs it — only single quotes suppress expansion, and those are rejected
   * outright. A Unix path is listed here for a simpler reason: quoting does not make {@code
   * /dev/null} exist on Windows.
   */
  private static final List<String[]> UNPORTABLE_ANYWHERE =
      List.of(
          new String[] {
            "$(", "command substitution", "compute the value in Java and concatenate it"
          },
          new String[] {
            "`", "command substitution", "compute the value in Java and concatenate it"
          },
          new String[] {"/dev/null", "a Unix device path", "the harness discards nothing you need"},
          new String[] {"/tmp/", "a Unix path", "use a workDir()-relative path"});

  /**
   * Rejects a cross-shell line that only works on bash.
   *
   * <p>Shell operators are checked outside quoted spans, so a URL carrying {@code &} inside double
   * quotes is fine — both shells protect it — while a bare pipe is not. Command substitution and
   * Unix paths are checked across the whole line, because double quotes do not protect those.
   */
  private static void requirePortableLine(String line) {
    StringBuilder unquoted = new StringBuilder();
    boolean inDouble = false;
    boolean inSingle = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '"' && !inSingle) {
        inDouble = !inDouble;
      } else if (c == '\'' && !inDouble) {
        if (!inSingle) {
          throw new AssertionError(
              "cmd.exe has no single-quote quoting, so '...' would reach the command verbatim: "
                  + line
                  + "\nUse double quotes, or run(List.of(...)) which quotes each argument for the "
                  + "shell in use.");
        }
        inSingle = false;
      } else if (!inDouble && !inSingle) {
        unquoted.append(c);
      }
    }
    String bare = unquoted.toString();
    for (String[] rule : UNPORTABLE_OPERATORS) {
      if (bare.contains(rule[0])) {
        throw new AssertionError(
            rule[1]
                + " ("
                + rule[0]
                + ") is not portable to cmd.exe: "
                + line
                + "\nInstead: "
                + rule[2]);
      }
    }
    for (String[] rule : UNPORTABLE_ANYWHERE) {
      if (line.contains(rule[0])) {
        throw new AssertionError(
            rule[1]
                + " ("
                + rule[0]
                + ") is not portable to cmd.exe, and double quotes do not protect it: "
                + line
                + "\nInstead: "
                + rule[2]);
      }
    }
    String[] words = bare.strip().split("\\s+");
    String command = words[0];
    if (command.contains("=")) {
      throw new AssertionError(
          "A leading NAME=value is bash-only syntax: "
              + line
              + "\nInstead: env(\""
              + command.substring(0, command.indexOf('='))
              + "\", ...) before the run().");
    }
    requirePortableTool(command);
  }

  /** Rejects a command that cmd.exe has no equivalent of, naming what to use instead. */
  private static void requirePortableTool(String command) {
    // The command's own name, with any directory prefix stripped: 'grep' and '/usr/bin/grep' are
    // the same mistake.
    String tool = command.substring(command.replace('\\', '/').lastIndexOf('/') + 1);
    String replacement = UNIX_ONLY_TOOLS.get(tool);
    if (replacement != null) {
      throw new AssertionError(
          "'"
              + tool
              + "' does not exist on cmd.exe, so this command could only ever pass on bash.\n"
              + "Instead: "
              + replacement
              + ".\nIf the test genuinely is shell-specific, mark it @EnabledOnShell, which "
              + "stands the rails down.");
    }
  }

  /** Resolves every variable spelling a cross-shell line may use, then rejects the cmd.exe one. */
  private String resolveForRun(String line) {
    requireOpen();
    String resolved =
        Variables.resolve(line, Variables.ANY, vars, "a command line", echoedPrefixes());
    Matcher cmdForm = Variables.CMD_FORM.matcher(resolved);
    if (cmdForm.find()) {
      throw new AssertionError(
          "cmd.exe-only variable "
              + cmdForm.group()
              + " in a cross-shell command line: "
              + line
              + "\nWrite ${"
              + cmdForm.group(1)
              + "} instead — the harness resolves that for every shell — or mark the test "
              + "@EnabledOnShell if it really is shell-specific.");
    }
    return resolved;
  }

  private CommandResult execute(String rawLine, Duration commandTimeout, boolean separateStreams) {
    return execute(rawLine, commandTimeout, separateStreams, null);
  }

  private CommandResult execute(
      String rawLine, Duration commandTimeout, boolean separateStreams, String stdin) {
    // stripTrailing so a """ text block whose closing delimiter sits on its own line — which
    // carries a trailing newline — behaves the same as a plain string.
    String line = rawLine.stripTrailing();
    Map<String, String> envDiff = envDiff();
    if (stdin != null) {
      transcript.note("on standard input:\n" + stdin);
    }
    ShellExecutor.Spec spec =
        new ShellExecutor.Spec(
            line,
            shell,
            cwd,
            vars,
            echoedPrefixes(),
            workDir,
            "cmd-" + SEQ.incrementAndGet(),
            commandTimeout,
            separateStreams,
            stdin,
            transcript);
    CommandResult result = ShellExecutor.execute(spec, startedProcesses::add);
    lastResult = result;
    transcript.record(result, envDiff);
    claimNewNodes();
    if (result.timedOut()) {
      throw new ShellAssertionError(
          "\n"
              + transcript.text()
              + "FAILED: command did not finish within "
              + commandTimeout.toSeconds()
              + "s and was killed\n");
    }
    return result;
  }

  /**
   * Only what this session changed relative to the resolved base environment — the full environment
   * is hundreds of entries and tells a reader nothing.
   *
   * <p>A session starts as a copy of the base, and {@link #env(String, String)} can only add or
   * replace, so an entry present in the base is always present here too; there is nothing to report
   * as unset.
   */
  private Map<String, String> envDiff() {
    Map<String, String> base = harnessEnv.baseEnv();
    Map<String, String> diff = new LinkedHashMap<>();
    vars.forEach(
        (k, v) -> {
          if (!v.equals(base.get(k))) {
            diff.put(k, v);
          }
        });
    return diff;
  }

  // ------------------------------------------------------- node ownership

  /**
   * Records any node of this class's that has appeared since the last check as belonging to this
   * session, so cleanup stops exactly the ones this scope started.
   *
   * <p>Asked of the running process list rather than of pid files, so it behaves the same on both
   * platforms and sees a node started in the foreground. Filtered to this class's port block,
   * because on the cmd side the underlying record is shared with every other class.
   */
  // owner != this compares session identity on purpose, so equals() would be wrong here.
  @SuppressWarnings("ReferenceEquality")
  private void claimNewNodes() {
    for (RunningProgram node : program.running(harnessEnv.marker())) {
      if (!harnessEnv.ports().contains(node.port())) {
        continue;
      }
      if (!registry.recordIfNew(node)) {
        continue;
      }

      // A node on a port an ancestor already owns stays the ancestor's, with its pid brought up to
      // date. Otherwise a test that restarts the node its class started in @BeforeClass would take
      // ownership of it, and stop it when that one test ended - tearing down the shared node every
      // later test in the class still expects to be running.
      CliSession owner = ownerOf(node.port());
      if (owner != null && owner != this) {
        owner.ownedNodes.put(node.port(), node.pid());
        if (log.isInfoEnabled()) {
          log.info(
              "Node on port {} restarted as pid {}; it stays owned by session '{}'",
              node.port(),
              node.pid(),
              owner.name);
        }
        continue;
      }

      ownedNodes.put(node.port(), node.pid());
      if (log.isInfoEnabled()) {
        log.info("Session '{}' now owns node on port {} (pid {})", name, node.port(), node.pid());
      }
    }
  }

  /**
   * The session in this scope's ancestry that already owns a node on {@code port}, or null.
   *
   * <p>Walks parents only, never children: ownership may move up to a longer-lived scope, so that
   * cleanup happens when that scope ends, but never down into a scope that is about to close.
   */
  private CliSession ownerOf(int port) {
    for (CliSession s = this; s != null; s = s.parent) {
      if (s.ownedNodes.containsKey(port)) {
        return s;
      }
    }
    return null;
  }

  /** The ports of the nodes this session started, in the order they appeared. */
  public Set<Integer> ownedPorts() {
    return new LinkedHashSet<>(ownedNodes.keySet());
  }

  // ---------------------------------------------------------------- helpers

  /**
   * Polls {@code condition} until it holds or {@code waitTimeout} elapses. Port of {@code
   * wait_for}.
   */
  public boolean waitFor(Duration waitTimeout, Duration interval, BooleanSupplier condition) {
    long deadline = System.nanoTime() + waitTimeout.toNanos();
    while (true) {
      if (condition.getAsBoolean()) {
        return true;
      }
      if (System.nanoTime() >= deadline) {
        return false;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(interval.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  /** As {@link #waitFor} but fails the test when the condition never holds. */
  public void awaitOrFail(
      String what, Duration waitTimeout, Duration interval, BooleanSupplier condition) {
    if (!waitFor(waitTimeout, interval, condition)) {
      throw new ShellAssertionError(
          "\n"
              + transcript.text()
              + "FAILED: timed out after "
              + waitTimeout.toSeconds()
              + "s waiting for "
              + what
              + "\n");
    }
  }

  // ---------------------------------------------------------------- cleanup

  /**
   * Stops everything this session started. Idempotent, and safe to call after a hook has already
   * thrown. Errors are collected and reported together rather than cutting the cleanup short.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    List<String> problems = new ArrayList<>();

    // Fixture web servers first: they hold only a loopback socket, and a test that failed
    // mid-fetch should not leave one listening.
    for (Integer port : net.servingPorts()) {
      try {
        serveHttpShutdown(port);
      } catch (RuntimeException e) {
        problems.add("stopping the web server on port " + port + ": " + e);
      }
    }

    // Graceful stop first, so the program gets to shut down cleanly and remove its own pid file.
    for (Map.Entry<Integer, Long> node : ownedNodes.entrySet()) {
      int port = node.getKey();
      Optional<String> stop = program.stopCommand(port);
      if (stop.isEmpty()) {
        continue;
      }
      try {
        CommandResult result = runForCleanup(stop.get());
        if (result != null && result.failed()) {
          log.warn("'{}' exited {}", stop.get(), result.status());
        }
      } catch (RuntimeException | AssertionError e) {
        problems.add("stopping the program on port " + port + ": " + e);
      }
    }

    // Anything still alive gets killed; the pid file is how we find it.
    for (Map.Entry<Integer, Long> node : ownedNodes.entrySet()) {
      long pid = node.getValue();
      if (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
        log.warn("pid {} (port {}) still alive after stop; killing", pid, node.getKey());
        if (!ProcessKill.killTree(pid, Duration.ofSeconds(10))) {
          problems.add("could not kill pid " + pid + " on port " + node.getKey());
        }
      }
      registry.forget(node.getKey(), pid);
      for (Path stale : program.staleStateFiles(harnessEnv.stateDir(), node.getKey())) {
        try {
          Files.deleteIfExists(stale);
        } catch (IOException e) {
          problems.add("could not delete " + stale + ": " + e);
        }
      }
    }

    // A command that overran its timeout may still have a live shell behind it.
    for (Process process : startedProcesses) {
      if (process.isAlive()) {
        ProcessKill.killTree(process.toHandle(), Duration.ofSeconds(10));
      }
    }

    deleteTempScripts();

    if (!problems.isEmpty()) {
      throw new AssertionError(
          "Shell session '" + name + "' cleanup had problems: " + String.join("; ", problems));
    }
  }

  /**
   * Runs a cleanup command without letting a timeout turn into a test failure, and without the
   * closed check that {@link #run} applies — cleanup runs after the session is already closed.
   */
  private CommandResult runForCleanup(String line) {
    try {
      return execute(line, Duration.ofMinutes(1), false);
    } catch (AssertionError e) {
      log.warn("Cleanup command failed: {}", line, e);
      return null;
    }
  }

  private void deleteTempScripts() {
    try (Stream<Path> files = Files.list(workDir)) {
      for (Path file : files.toList()) {
        if (file.getFileName().toString().endsWith(".cmd")) {
          Files.deleteIfExists(file);
        }
      }
    } catch (IOException e) {
      log.warn("Could not clean temp scripts in {}", workDir, e);
    }
  }

  private static Map<String, String> newEnvMap() {
    return isWindows() ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new LinkedHashMap<>();
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");
  }

  private static String sanitize(String raw) {
    return raw.replaceAll("[^A-Za-z0-9._-]", "_");
  }

  private static void createDir(Path dir) {
    try {
      Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not create " + dir, e);
    }
  }

  @Override
  public String toString() {
    return "CliSession["
        + name
        + (parent == null ? ", class scope" : ", child of " + parent.name)
        + "]";
  }
}
