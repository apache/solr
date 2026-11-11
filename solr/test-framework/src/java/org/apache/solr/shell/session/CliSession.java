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

package org.apache.solr.shell.session;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.solr.shell.env.HarnessEnv;
import org.apache.solr.shell.env.Ports;
import org.apache.solr.shell.exec.CommandResult;
import org.apache.solr.shell.exec.PidFile;
import org.apache.solr.shell.exec.ProcessKill;
import org.apache.solr.shell.exec.Shell;
import org.apache.solr.shell.exec.ShellExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A shell session: the environment, working directory, shell and timeouts that every command in a
 * scope runs with, plus the state needed to clean up afterwards.
 *
 * <p>There is no long-lived shell process. Every {@link #run} starts a fresh one with this
 * session's environment and cwd, so quoting, globbing and variable expansion are done by the real
 * shell while control flow stays in Java. A class-scoped session stands in for BATS' {@code
 * setup_file}; each test gets a {@linkplain #child child} of it whose environment and cwd changes
 * are discarded afterwards.
 *
 * <p>Sessions track which Solr nodes they started, by diffing {@code SOLR_PID_DIR} after each
 * command, and stop exactly those on {@link #close()}.
 */
public class CliSession implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * {@code {{NAME}}} alone — the only form substituted inside a shell-specific {@code script}.
   *
   * <p>Double braces are inert in both bash, which only expands braces containing a comma or {@code
   * ..}, and cmd.exe — unlike {@code $NAME} and {@code %NAME%}.
   */
  private static final Pattern TEMPLATE = Pattern.compile("\\{\\{([A-Za-z_][A-Za-z0-9_]*)}}");

  /**
   * Every variable spelling a cross-shell command line may use: {@code {{NAME}}}, {@code ${NAME}}
   * and {@code $NAME}. A {@code run} line has no shell state of its own — one process per command,
   * no locals, no positional parameters — so a {@code $NAME} in one can only ever mean an
   * environment variable, which is why reading it as one is safe.
   *
   * <p>{@code $'..'}, {@code $1} and a bare {@code $} do not match and are left for the shell.
   */
  private static final Pattern VARIABLE =
      Pattern.compile(
          "\\{\\{([A-Za-z_][A-Za-z0-9_]*)}}"
              + "|\\$\\{([A-Za-z_][A-Za-z0-9_]*)}"
              + "|\\$([A-Za-z_][A-Za-z0-9_]*)");

  /**
   * The cmd.exe spelling. Not substituted, because {@code %} collides with {@link String#format},
   * but rejected so that a line written this way cannot pass on Windows and fail on bash.
   */
  private static final Pattern CMD_VARIABLE = Pattern.compile("%([A-Za-z_][A-Za-z0-9_]*)%");

  /** Default per-command timeout. */
  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(120);

  /** Timeout for lines that start or restart a Solr node, which is much slower. */
  public static final Duration DEFAULT_START_TIMEOUT = Duration.ofSeconds(300);

  /** Every PID the harness has ever seen, so the shutdown hook can finish the job. */
  private static final Set<Long> ALL_RECORDED_PIDS = ConcurrentHashMap.newKeySet();

  /** PID files already accounted for, so a node is owned by the scope that actually started it. */
  private static final Map<Integer, Long> KNOWN_PID_FILES = new ConcurrentHashMap<>();

  private static final AtomicInteger SEQ = new AtomicInteger();

  static {
    Runtime.getRuntime()
        .addShutdownHook(new Thread(CliSession::killAllRecordedPids, "solr-shell-harness-cleanup"));
  }

  private final HarnessEnv harnessEnv;
  private final CliSession parent;
  private final String name;
  private final Path workDir;
  private final Transcript transcript;
  private final Map<String, String> vars;
  private final Shell shell;

  private final List<Process> startedProcesses = new ArrayList<>();
  private final Map<Integer, Long> ownedNodes = new LinkedHashMap<>();

  private Path cwd;
  private Duration timeout = DEFAULT_TIMEOUT;
  private Duration startTimeout = DEFAULT_START_TIMEOUT;
  private CommandResult lastResult;
  private boolean closed;

  private CliSession(
      HarnessEnv harnessEnv,
      CliSession parent,
      String name,
      Path workDir,
      Map<String, String> vars,
      Path cwd,
      Shell shell) {
    this.harnessEnv = harnessEnv;
    this.parent = parent;
    this.name = name;
    this.workDir = workDir;
    this.vars = vars;
    this.cwd = cwd;
    this.shell = shell;
    this.transcript = new Transcript(workDir.resolve("transcript.log"));
  }

  /**
   * Creates the class-scoped session: BATS' {@code setup_file} scope. Its work dir is {@code
   * $TEST_OUTPUT_DIR/work/<name>}.
   */
  public static CliSession forScope(HarnessEnv env, String name) {
    Path workDir = env.testOutputDir().resolve("work").resolve(sanitize(name));
    createDir(workDir);
    Map<String, String> vars = newEnvMap();
    vars.putAll(env.baseEnv());
    return new CliSession(
        env, null, name, workDir, vars, env.testOutputDir(), Shell.defaultShell());
  }

  /**
   * Creates a child session for one test: BATS' {@code setup} scope. It starts with a copy of this
   * session's environment and cwd, so nothing a test changes leaks into the next one.
   */
  public CliSession child(String childName) {
    Path childWorkDir = workDir.resolve(sanitize(childName));
    createDir(childWorkDir);
    Map<String, String> childVars = newEnvMap();
    childVars.putAll(vars);
    CliSession child =
        new CliSession(harnessEnv, this, childName, childWorkDir, childVars, cwd, shell);
    child.timeout = timeout;
    child.startTimeout = startTimeout;
    return child;
  }

  // ---------------------------------------------------------------- state

  /** Sets an environment variable for subsequent commands in this session. */
  public CliSession env(String key, String value) {
    vars.put(key, value);
    return this;
  }

  /** The current value of an environment variable, or null. */
  public String env(String key) {
    return vars.get(key);
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

  public Ports ports() {
    return harnessEnv.ports();
  }

  public HarnessEnv harnessEnv() {
    return harnessEnv;
  }

  public Transcript transcript() {
    return transcript;
  }

  /** Overrides the default per-command timeout for this session. */
  public CliSession timeout(Duration newTimeout) {
    this.timeout = newTimeout;
    return this;
  }

  /** Overrides the timeout used for {@code solr start} and {@code solr restart} lines. */
  public CliSession startTimeout(Duration newStartTimeout) {
    this.startTimeout = newStartTimeout;
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
   * <p>Environment variables are written the way the BATS suites write them, so a ported line needs
   * no edit at all:
   *
   * <pre>
   *   run("solr zk ls / -z localhost:${ZK_PORT}");
   *   run("solr start -p $SOLR2_PORT");
   * </pre>
   *
   * <p>{@code ${NAME}}, {@code $NAME} and {@code {{NAME}}} all resolve against the session
   * environment — {@code SOLR_PORT}, {@code ZK_PORT}, {@code SOLR_TIP} and the rest, plus anything
   * {@link #env(String, String)} has set.
   *
   * <p>The <em>harness</em> substitutes them, not the shell. That is what makes one line work on
   * both: bash would expand {@code $NAME} and cmd.exe would not, and cmd.exe wants {@code %NAME%},
   * which bash would not expand. Resolving in Java also means an unknown name fails the test
   * instead of quietly becoming the empty string, and that the transcript records the real command.
   * It is sound to read {@code $NAME} as an environment variable here because a {@code run} line
   * has no shell state of its own — one process per command, so there are no locals or positional
   * parameters it could refer to.
   *
   * <p>{@code %NAME%} is rejected rather than substituted, because {@code %} is also {@link
   * String#format}'s. {@code $'..'}, {@code $1} and a bare {@code $} are left for the shell.
   *
   * <p>{@code fmtArgs}, when given, are applied first with {@link String#format} under {@code
   * Locale.ROOT}, for values that come from Java rather than the environment.
   */
  public CommandResult run(String line, Object... fmtArgs) {
    String resolved = resolveForRun(format(line, fmtArgs));
    return execute(resolved, timeoutFor(resolved), false);
  }

  /** Runs a command line with an explicit timeout. */
  public CommandResult run(Duration commandTimeout, String line, Object... fmtArgs) {
    return execute(resolveForRun(format(line, fmtArgs)), commandTimeout, false);
  }

  /**
   * Runs an argv, letting the harness quote each argument for the current shell. Each argument has
   * its variables resolved before it is quoted.
   */
  public CommandResult run(List<String> argv) {
    String line = shell.joinArgs(argv.stream().map(this::resolveForRun).toList());
    return execute(line, timeoutFor(line), false);
  }

  /** Like {@link #run(String, Object...)} but keeps {@code stdout()} and {@code stderr()} apart. */
  public CommandResult runSeparated(String line, Object... fmtArgs) {
    String resolved = resolveForRun(format(line, fmtArgs));
    return execute(resolved, timeoutFor(resolved), true);
  }

  /**
   * Escape hatch for shell-specific, possibly multi-line logic. The script for the current shell
   * runs as a single process; passing null for it means the test cannot run here.
   *
   * <p>Here {@code $NAME}, {@code %NAME%} and the rest are left entirely to the shell — a script
   * has its own locals, loop variables and positional parameters, and you have already committed to
   * one shell by writing two bodies. {@code {{NAME}}} is the one form the harness still resolves,
   * for when you want a harness value in a script without the shell seeing a variable at all.
   *
   * @throws org.junit.AssumptionViolatedException if the current shell has no script
   */
  public CommandResult script(String bashScript, String cmdScript) {
    requireOpen();
    String chosen = shell == Shell.BASH ? bashScript : cmdScript;
    if (chosen == null) {
      throw new org.junit.AssumptionViolatedException(
          "No " + shell + " script supplied for this test");
    }
    return execute(interpolate(chosen, TEMPLATE), timeout, false);
  }

  private static String format(String line, Object... fmtArgs) {
    return fmtArgs.length == 0 ? line : String.format(Locale.ROOT, line, fmtArgs);
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Session '" + name + "' is already closed");
    }
  }

  /** Resolves every variable spelling a cross-shell line may use, then rejects the cmd.exe one. */
  private String resolveForRun(String line) {
    requireOpen();
    String resolved = interpolate(line, VARIABLE);
    Matcher cmdForm = CMD_VARIABLE.matcher(resolved);
    if (cmdForm.find()) {
      throw new AssertionError(
          "cmd.exe-only variable "
              + cmdForm.group()
              + " in a cross-shell command line: "
              + line
              + "\nWrite ${"
              + cmdForm.group(1)
              + "} instead — the harness resolves that for every shell — or move the line into "
              + "script(bash, cmd) if it really is shell-specific.");
    }
    return resolved;
  }

  /**
   * Replaces each variable reference {@code pattern} matches with the session environment variable
   * it names. An unknown name is a test failure, not the empty string a shell would give you.
   */
  private String interpolate(String line, Pattern pattern) {
    Matcher m = pattern.matcher(line);
    StringBuilder resolved = new StringBuilder();
    while (m.find()) {
      String key = null;
      for (int group = 1; group <= m.groupCount() && key == null; group++) {
        key = m.group(group);
      }
      String value = vars.get(key);
      if (value == null) {
        throw new AssertionError(
            "Unknown variable "
                + m.group()
                + " in command line: "
                + line
                + "\nHarness variables available: "
                + harnessEnv.vars().keySet()
                + " (plus the inherited process environment and anything env(..) set)");
      }
      m.appendReplacement(resolved, Matcher.quoteReplacement(value));
    }
    m.appendTail(resolved);
    return resolved.toString();
  }

  private Duration timeoutFor(String line) {
    String trimmed = line.trim();
    return trimmed.startsWith("solr start") || trimmed.startsWith("solr restart")
        ? startTimeout
        : timeout;
  }

  private CommandResult execute(String rawLine, Duration commandTimeout, boolean separateStreams) {
    // stripTrailing so a """ text block whose closing delimiter sits on its own line — which
    // carries a trailing newline — behaves the same as a plain string.
    String line = rawLine.stripTrailing();
    Map<String, String> envDiff = envDiff();
    ShellExecutor.Spec spec =
        new ShellExecutor.Spec(
            line,
            shell,
            cwd,
            vars,
            workDir,
            "cmd-" + SEQ.incrementAndGet(),
            commandTimeout,
            separateStreams,
            harnessEnv.picocli());
    CommandResult result = ShellExecutor.execute(spec, startedProcesses::add);
    lastResult = result;
    transcript.record(result, envDiff);
    claimNewNodes();
    if (result.timedOut()) {
      throw new AssertionError(
          "Command did not finish within "
              + commandTimeout.toSeconds()
              + "s and was killed\n\n"
              + result.transcriptEntry());
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
   * Records any {@code solr-<port>.pid} file that appeared since the last check as belonging to
   * this session. The PID file, not the process tree, is the source of truth: a detached Solr node
   * is re-parented as soon as its launching shell exits.
   */
  private void claimNewNodes() {
    try {
      for (PidFile pidFile : PidFile.listIn(harnessEnv.pidDir())) {
        OptionalLong read = pidFile.pid();
        if (read.isEmpty()) {
          continue;
        }
        long pid = read.getAsLong();
        int port = pidFile.port();
        Long known = KNOWN_PID_FILES.get(port);
        if (known != null && known == pid) {
          continue;
        }
        KNOWN_PID_FILES.put(port, pid);
        ALL_RECORDED_PIDS.add(pid);
        ownedNodes.put(port, pid);
        log.info("Session '{}' now owns Solr node on port {} (pid {})", name, port, pid);
      }
    } catch (IOException e) {
      log.warn("Could not scan {} for Solr pid files", harnessEnv.pidDir(), e);
    }
  }

  /** The ports of the Solr nodes this session started, in the order they appeared. */
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
      throw new AssertionError(
          "Timed out after " + waitTimeout.toSeconds() + "s waiting for: " + what);
    }
  }

  /** The response to a harness HTTP request. */
  public record HttpResult(int status, String body) {}

  /**
   * A GET against {@code http://localhost:<SOLR_PORT><pathAndQuery>}. Replaces the {@code curl}
   * calls in the BATS suites, which is what makes readiness checks work on Windows.
   */
  public HttpResult http(String pathAndQuery) {
    return http(ports().solr(), pathAndQuery);
  }

  /** A GET against an explicit port on localhost. */
  public HttpResult http(int port, String pathAndQuery) {
    String url = "http://localhost:" + port + pathAndQuery;
    try (HttpClient client =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(30)).GET().build();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      return new HttpResult(response.statusCode(), response.body());
    } catch (IOException e) {
      return new HttpResult(-1, e.toString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted requesting " + url, e);
    }
  }

  /** Whether the named collection is in ZooKeeper. Port of {@code collection_exists}. */
  public boolean collectionExists(String collection) {
    return zkChildren("/collections").contains(collection);
  }

  /** Whether the named config set is in ZooKeeper. Port of {@code config_exists}. */
  public boolean configExists(String configName) {
    return zkChildren("/configs").contains(configName);
  }

  private List<String> zkChildren(String zkPath) {
    CommandResult result = run("solr zk ls %s -z localhost:%d", zkPath, ports().zk());
    if (result.failed()) {
      return List.of();
    }
    return result.lines().stream().map(String::trim).filter(s -> !s.isEmpty()).toList();
  }

  /** Port of {@code delete_all_collections}. */
  public void deleteAllCollections() {
    for (String collection : zkChildren("/collections")) {
      run("solr delete -c %s --delete-config", collection);
    }
  }

  /** Port of {@code shutdown_all}: {@code solr stop --all}, ignoring the outcome. */
  public void shutdownAll() {
    try {
      run(Duration.ofSeconds(120), "solr stop --all");
    } catch (RuntimeException | AssertionError e) {
      log.warn("'solr stop --all' failed during cleanup", e);
    }
  }

  /** Prints the tail of {@code solr.log} to stderr, for debugging a failing test. */
  public void dumpSolrLog() {
    Path logFile = harnessEnv.logsDir().resolve("solr.log");
    if (!Files.isRegularFile(logFile)) {
      System.err.println("No solr.log at " + logFile);
      return;
    }
    try {
      List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
      List<String> tail = lines.subList(Math.max(0, lines.size() - 200), lines.size());
      System.err.println("--- tail of " + logFile + " ---");
      tail.forEach(System.err::println);
    } catch (IOException e) {
      System.err.println("Could not read " + logFile + ": " + e);
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

    // Graceful stop first, so Solr gets to shut down cleanly and remove its own pid file.
    for (Map.Entry<Integer, Long> node : ownedNodes.entrySet()) {
      int port = node.getKey();
      try {
        CommandResult result = runForCleanup("solr stop -p " + port);
        if (result != null && result.failed()) {
          log.warn("'solr stop -p {}' exited {}", port, result.status());
        }
      } catch (RuntimeException | AssertionError e) {
        problems.add("stopping Solr on port " + port + ": " + e);
      }
    }

    // Anything still alive gets killed; the pid file is how we find it.
    for (Map.Entry<Integer, Long> node : ownedNodes.entrySet()) {
      long pid = node.getValue();
      if (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
        log.warn("Solr pid {} (port {}) still alive after stop; killing", pid, node.getKey());
        if (!ProcessKill.killTree(pid, Duration.ofSeconds(10))) {
          problems.add("could not kill Solr pid " + pid + " on port " + node.getKey());
        }
      }
      ALL_RECORDED_PIDS.remove(pid);
      KNOWN_PID_FILES.remove(node.getKey(), pid);
      Path pidFile = harnessEnv.pidDir().resolve("solr-" + node.getKey() + ".pid");
      try {
        Files.deleteIfExists(pidFile);
      } catch (IOException e) {
        problems.add("could not delete " + pidFile + ": " + e);
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
      return execute(line, Duration.ofSeconds(60), false);
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

  /** Last resort, from the JVM shutdown hook: kill every Solr the harness ever started. */
  private static void killAllRecordedPids() {
    for (Long pid : ALL_RECORDED_PIDS) {
      if (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
        ProcessKill.killTree(pid, Duration.ofSeconds(5));
      }
    }
  }

  /** Forgets the PID files seen so far, so a fresh class starts with a clean slate. */
  public static void resetKnownNodes() {
    KNOWN_PID_FILES.clear();
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
