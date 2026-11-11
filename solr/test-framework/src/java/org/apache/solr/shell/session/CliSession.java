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

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.SimpleFileServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.cli.SolrProcessManager.SolrProcess;
import org.apache.solr.shell.env.HarnessEnv;
import org.apache.solr.shell.env.Ports;
import org.apache.solr.shell.exec.CommandResult;
import org.apache.solr.shell.exec.ProcessKill;
import org.apache.solr.shell.exec.Shell;
import org.apache.solr.shell.exec.ShellAssertionError;
import org.apache.solr.shell.exec.ShellExecutor;
import org.apache.solr.shell.exec.SolrNodes;
import org.apache.solr.shell.exec.Variables;
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
 * <p>Sessions track which Solr nodes they started by scanning the running process list after each
 * command, filtered to this scope's marker and port block, and stop exactly those on {@link
 * #close()}.
 */
public class CliSession implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Default per-command timeout. */
  public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(120);

  /** Timeout for lines that start or restart a Solr node, which is much slower. */
  public static final Duration DEFAULT_START_TIMEOUT = Duration.ofSeconds(300);

  /** Timeout for {@link #download}, which fetches from the network rather than from a node. */
  private static final Duration DOWNLOAD_TIMEOUT = Duration.ofMinutes(10);

  /** Every PID the harness has ever seen, so the shutdown hook can finish the job. */
  private static final Set<Long> ALL_RECORDED_PIDS = ConcurrentHashMap.newKeySet();

  /** Nodes already accounted for, so one is owned by the scope that actually started it. */
  private static final Map<Integer, Long> KNOWN_NODES = new ConcurrentHashMap<>();

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
  private final SolrCliHelpers helpers = new SolrCliHelpers(this);
  private final Map<String, String> vars;
  private final Shell shell;

  private final List<Process> startedProcesses = new ArrayList<>();
  private final Map<Integer, Long> ownedNodes = new LinkedHashMap<>();
  private final Map<Integer, HttpServer> fileServers = new LinkedHashMap<>();

  private Path cwd;
  private Duration timeout = DEFAULT_TIMEOUT;
  private Duration startTimeout = DEFAULT_START_TIMEOUT;
  private CommandResult lastResult;
  private boolean closed;
  private boolean shellSpecific;

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
    // Exposed as a variable so a test can name its own scratch space inside a command line or an
    // assertion — ${WORK_DIR}/something — instead of formatting a Java path into the string.
    this.vars.put("WORK_DIR", workDir.toString());
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
    // Starts in its own work directory, so a relative path in a command means something per
    // scope and a test needs no cd() to create or name a scratch file.
    return new CliSession(env, null, name, workDir, vars, workDir, Shell.defaultShell());
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
    // The child gets its own work directory as its cwd, unless the parent had cd'd elsewhere.
    Path childCwd = cwd.equals(workDir) ? childWorkDir : cwd;
    CliSession child =
        new CliSession(harnessEnv, this, childName, childWorkDir, childVars, childCwd, shell);
    child.timeout = timeout;
    child.startTimeout = startTimeout;
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
   * Removes an environment variable for subsequent commands, as BATS' {@code unset} does.
   *
   * <p>Needed because several CLI behaviours are only reachable with a variable absent rather than
   * empty — {@code bin/solr} checks whether {@code SOLR_PORT} is set at all before falling back to
   * its default.
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
    return java.util.Collections.unmodifiableMap(vars);
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
   * Creates an empty file, as BATS' {@code touch} does, resolving a relative path against {@link
   * #cwd()} and creating any missing parent directories.
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
  // The shell file tools a BATS test reaches for — mkdir, rm, cp, cat, ls, find, wc — exist on
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
        Pattern.compile(Variables.resolve(regex, Variables.ANY, vars, "a file assertion"));
    return read(path).lines().filter(line -> pattern.matcher(line).find()).toList();
  }

  /** Port of {@code grep -q <text> <path>}. */
  public boolean fileContains(String path, String text) {
    return read(path).contains(Variables.resolve(text, Variables.ANY, vars, "a file assertion"));
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
        IOUtils.rm(target);
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
   * {@code dir}, so {@code "*.txt"} lists one level and {@code "**&#47;*.xml"} walks the tree.
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

  /**
   * Serves a directory over HTTP on a free port, and returns the port.
   *
   * <p>For a test whose subject is Solr reading a URL — {@code solr post --mode web}, a remote
   * config set, a module fetching a resource. Pointing such a test at a real site makes it depend
   * on the network and on that site's current content; serving a snapshot locally does not, and it
   * still exercises the whole HTTP path.
   *
   * <p>The port is an ephemeral one the OS picks, not one from this class's block, because a web
   * server is not a Solr node: nothing else needs to predict its number, and asking the OS avoids
   * spending one of the four ports a class owns.
   *
   * <p>Every server a scope starts is stopped when the scope ends, so a test needs no teardown.
   * {@link #serveHttpShutdown(int)} stops one early, for a test that wants to observe a fetch
   * failing.
   *
   * @param webRoot the directory to serve, resolved against {@link #cwd()}
   * @return the port it is listening on
   */
  @SuppressForbidden(reason = "com.sun.net.httpserver is where the JDK's own web server lives")
  public int serveHttp(String webRoot) {
    requireOpen();
    Path root = path(webRoot);
    if (!Files.isDirectory(root)) {
      throw new AssertionError("serveHttp: not a directory: " + root);
    }
    // The JDK's own file server, JEP 408. Nothing to get wrong here: it maps the path, picks the
    // content type, serves index.html for a directory and refuses anything outside the root.
    // Bound to loopback — a fixture has no business being reachable from off the machine — and to
    // port 0, meaning whatever is free.
    HttpServer server =
        SimpleFileServer.createFileServer(
            new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
            root.toAbsolutePath(),
            SimpleFileServer.OutputLevel.NONE);
    server.start();
    int port = server.getAddress().getPort();
    fileServers.put(port, server);
    transcript.note("serving " + root + " at http://localhost:" + port);
    return port;
  }

  /** Stops one server started by {@link #serveHttp}. Stopping one twice is not an error. */
  @SuppressForbidden(reason = "com.sun.net.httpserver is where the JDK's own web server lives")
  public void serveHttpShutdown(int port) {
    HttpServer server = fileServers.remove(port);
    if (server != null) {
      server.stop(0);
    }
  }

  /**
   * Port of {@code curl -o <path> <url>}: fetches a URL into a file, creating parent directories.
   *
   * <p>In Java because neither {@code curl} nor {@code wget} is on a stock Windows runner. A
   * non-2xx response, or a transport failure, fails the test rather than leaving a truncated file
   * behind.
   *
   * @return the absolute path of the downloaded file
   */
  public Path download(String url, String path) {
    Path file = path(path);
    try (HttpClient client =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build()) {
      Files.createDirectories(file.getParent());
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(url)).timeout(DOWNLOAD_TIMEOUT).GET().build();
      HttpResponse<Path> response =
          client.send(
              request,
              HttpResponse.BodyHandlers.ofFile(
                  file,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.WRITE,
                  StandardOpenOption.TRUNCATE_EXISTING));
      if (response.statusCode() / 100 != 2) {
        throw new AssertionError(
            "Downloading " + url + " answered HTTP " + response.statusCode() + ", not 2xx");
      }
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException("Could not download " + url + " to " + file, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted downloading " + url, e);
    }
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

  /** The Solr-aware conveniences for this session: collections, configs, HTTP, log dumps. */
  public SolrCliHelpers helpers() {
    return helpers;
  }

  /**
   * Turns off the cross-shell portability rails for this session.
   *
   * <p>For a test that has already declared itself one shell's with {@link
   * org.apache.solr.shell.EnabledOnShell} — set automatically there. Such a test may use that
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
   * <p>A value that comes from Java rather than the environment is simply concatenated — {@code
   * run("solr start -p " + myPort)} — which reads better than any format placeholder. One sharp
   * edge: concatenation happens before the harness resolves the line, so a value that itself
   * contains {@code ${...}} is taken as a variable reference.
   *
   * <p>An argument needing per-shell quoting goes through {@link #run(List)} instead, which quotes
   * each element correctly for bash and for cmd.exe.
   */
  public CommandResult run(String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    String resolved = resolveForRun(line);
    return execute(resolved, timeoutFor(resolved), false);
  }

  /** Runs a command line with an explicit timeout. */
  public CommandResult run(Duration commandTimeout, String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    return execute(resolveForRun(line), commandTimeout, false);
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
    return execute(line, timeoutFor(line), false);
  }

  /** Like {@link #run(String)} but keeps {@code stdout()} and {@code stderr()} apart. */
  public CommandResult runSeparated(String line) {
    requireSingleCommand(line);
    if (!shellSpecific) {
      requirePortableLine(line);
    }
    String resolved = resolveForRun(line);
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
    if (shell == Shell.CMD) {
      // cmd.exe has no $NAME syntax at all, so one in a cmd body is dead text that will reach the
      // command verbatim. Worth catching, because the bash body of the same script() call expands
      // it from the environment and therefore appears to work — exactly how the stdin test in the
      // test_post.bats port came to pass on bash and fail on Windows.
      Matcher stray = Variables.ANY.matcher(chosen);
      while (stray.find()) {
        if (stray.group().startsWith("$")) {
          throw new AssertionError(
              "cmd.exe does not expand "
                  + stray.group()
                  + ", so it would reach the command as text. Write {{"
                  + stray.group().replaceAll("[${}]", "")
                  + "}} to have the harness resolve it, or %NAME% if the shell really should.\n"
                  + chosen);
        }
      }
    }
    return execute(
        Variables.resolve(chosen, Variables.BRACES_ONLY, vars, "a script"), timeout, false);
  }

  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException("Session '" + name + "' is already closed");
    }
  }

  /**
   * A run is one command. A newline in the line would change what cmd.exe does without saying so:
   * Shell.CMD drops the {@code call} for a multi-line body, and without {@code call} a batch file
   * that invokes {@code solr.cmd} never gets control back. Easy to write by accident in a text
   * block.
   *
   * <p>Only the string form is checked. An argv element may legitimately contain a newline — the
   * shell quoting takes care of it — which is how a BATS line like {@code $'id,value\nROW1,0.47'}
   * ports over.
   */
  private void requireSingleCommand(String line) {
    if (line.strip().contains("\n")) {
      throw new AssertionError(
          "A run() line must be a single command; use script(bashBody, cmdBody) for several: "
              + line);
    }
  }

  /**
   * The Unix-only tools a BATS line reaches for, each with the harness feature that replaces it.
   *
   * <p>None of these exist on cmd.exe, so a line invoking one is a test that silently only works on
   * one platform. Caught here rather than on Windows three weeks later, and with the replacement
   * named, because the replacement is the whole point: the harness grew {@link #read}, {@link
   * #list}, {@code linesMatching} and the rest precisely so a port does not have to shell out.
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
          Map.entry("ps", "the harness tracks what a test started; see solr status"),
          Map.entry("kill", "the harness stops what a test started; see solr stop"),
          Map.entry("pkill", "the harness stops what a test started; see solr stop"),
          Map.entry("pgrep", "the harness tracks what a test started"),
          Map.entry("lsof", "the harness owns its port block; no process lookup needed"),
          Map.entry("chmod", "no cmd.exe equivalent — mark the test @EnabledOnShell(Shell.BASH)"),
          Map.entry("chown", "no cmd.exe equivalent — mark the test @EnabledOnShell(Shell.BASH)"),
          Map.entry("ln", "Files.createSymbolicLink, and @EnabledOnShell(Shell.BASH)"),
          Map.entry("openssl", "no cmd.exe equivalent — mark the test @EnabledOnShell(Shell.BASH)"),
          Map.entry("tar", "unpack it in Java, or mark the test @EnabledOnShell(Shell.BASH)"),
          Map.entry("unzip", "unpack it in Java, or mark the test @EnabledOnShell(Shell.BASH)"),
          Map.entry("xargs", "loop in Java"),
          Map.entry("tee", "the harness already captures the output"),
          Map.entry("which", "no cmd.exe equivalent"),
          Map.entry("basename", "path(p).getFileName()"),
          Map.entry("dirname", "path(p).getParent()"),
          Map.entry("realpath", "path(p), which normalizes"),
          Map.entry("pwd", "cwd()"),
          Map.entry("export", "env(name, value)"),
          Map.entry("unset", "unset(name)"),
          Map.entry("source", "script(bashBody, cmdBody) if it really is shell-specific"),
          Map.entry("eval", "script(bashBody, cmdBody) if it really is shell-specific"));

  /**
   * Shell operators with no cmd.exe equivalent. Double quotes neutralise all of these, so they are
   * only a problem outside a quoted span — a URL carrying {@code &} in quotes is fine.
   */
  private static final List<String[]> UNPORTABLE_OPERATORS =
      List.of(
          new String[] {
            "|", "a pipe", "assert on the command's own output, or use script(bashBody, cmdBody)"
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
              + ".\nIf the test genuinely is shell-specific, use script(bashBody, cmdBody) and "
              + "@EnabledOnShell.");
    }
  }

  /** Resolves every variable spelling a cross-shell line may use, then rejects the cmd.exe one. */
  private String resolveForRun(String line) {
    requireOpen();
    String resolved = Variables.resolve(line, Variables.ANY, vars, "a command line");
    Matcher cmdForm = Variables.CMD_FORM.matcher(resolved);
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
  private void claimNewNodes() {
    for (SolrProcess node : SolrNodes.running(harnessEnv.marker())) {
      if (!harnessEnv.ports().contains(node.port())) {
        continue;
      }
      Long known = KNOWN_NODES.get(node.port());
      if (known != null && known == node.pid()) {
        continue;
      }
      KNOWN_NODES.put(node.port(), node.pid());
      ALL_RECORDED_PIDS.add(node.pid());

      // A node on a port an ancestor already owns stays the ancestor's, with its pid brought up to
      // date. Otherwise a test that restarts the node its class started in @BeforeClass would take
      // ownership of it, and stop it when that one test ended - tearing down the shared node every
      // later test in the class still expects to be running.
      CliSession owner = ownerOf(node.port());
      if (owner != null && owner != this) {
        owner.ownedNodes.put(node.port(), node.pid());
        if (log.isInfoEnabled()) {
          log.info(
              "Solr node on port {} restarted as pid {}; it stays owned by session '{}'",
              node.port(),
              node.pid(),
              owner.name);
        }
        continue;
      }

      ownedNodes.put(node.port(), node.pid());
      if (log.isInfoEnabled()) {
        log.info(
            "Session '{}' now owns Solr node on port {} (pid {})", name, node.port(), node.pid());
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
    for (Integer port : List.copyOf(fileServers.keySet())) {
      try {
        serveHttpShutdown(port);
      } catch (RuntimeException e) {
        problems.add("stopping the web server on port " + port + ": " + e);
      }
    }

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
      KNOWN_NODES.remove(node.getKey(), pid);
      // Both spellings: bin/solr writes solr-<port>.pid holding a PID, bin\solr.cmd writes
      // solr-<port>.port holding a port, and SolrProcessManager reads either
      // (pidFilePattern = ^solr-([0-9]+)\.(pid|port)$). Only the graceful stop cleans up after
      // itself, so a force-killed node leaves its file behind — and a stale file in this class's
      // SOLR_PID_DIR is what 'solr stop --all' and 'solr status' would then walk.
      for (String suffix : List.of(".pid", ".port")) {
        Path pidFile = harnessEnv.pidDir().resolve("solr-" + node.getKey() + suffix);
        try {
          Files.deleteIfExists(pidFile);
        } catch (IOException e) {
          problems.add("could not delete " + pidFile + ": " + e);
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

  /** Forgets the nodes seen so far, so a fresh class starts with a clean slate. */
  public static void resetKnownNodes() {
    KNOWN_NODES.clear();
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
