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

package org.apache.solr.cats.env;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.solr.cats.exec.ProgramRegistry;
import org.apache.solr.cats.exec.ProgramUnderTest;

/**
 * The environment one scope of a shell test run operates in: which installation is under test,
 * where it may write, which ports are ours, the marker its processes carry, and the base
 * environment handed to every spawned shell.
 *
 * <p>Resolved per test class, so classes running concurrently in separate forks share neither a
 * directory to write in nor a port block.
 *
 * <p>Two kinds of setting meet here and are kept apart on purpose:
 *
 * <ul>
 *   <li><b>The harness' own</b> — the output directory, the port base, the command timeout — are
 *       named {@code CATS_*} and read through {@link Lookup#CATS}. None of them is exported to the
 *       program; the harness has no ports and no home of its own to tell it about.
 *   <li><b>The program's</b> — every variable it actually reads — are assembled by {@link
 *       #assemble} in the subclass, which is the only place that knows their names, and are looked
 *       up through a chain carrying {@linkplain #lookupPrefix() the project's prefix}.
 * </ul>
 */
public abstract class CatsHarnessEnv {

  /**
   * The default a command is given to finish in, when {@code CATS_TIMEOUT} / {@code -Dcats.timeout}
   * does not say otherwise. That setting is a whole number of seconds.
   */
  public static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

  private final ProgramUnderTest program;
  private final Lookup lookup;
  private final Optional<Path> installDir;
  private final Path outputDir;
  private final Path classDir;
  private final Path failureDir;
  private final PortBlock ports;
  private final String marker;
  private final Duration timeout;
  private final Map<String, String> vars;
  private final Map<String, String> baseEnv;
  private final ProgramRegistry registry = new ProgramRegistry();

  /**
   * Resolves one scope's environment.
   *
   * @param program what is being driven: where its launcher lives, which ports it wants
   * @param scopeName the test class this scope belongs to, which keys the directory it may write
   *     in; null puts that directory at the output directory itself, which is how the harness' own
   *     tests run without a per-class scope
   * @param explicitInstallDir the installation to test, bypassing the lookup chain; null to look it
   *     up under {@link #installDirVariable()}
   * @param explicitOutputDir where every scope of this run writes, bypassing the lookup chain; null
   *     to look it up under {@code CATS_OUTPUT_DIR}
   */
  protected CatsHarnessEnv(
      ProgramUnderTest program, String scopeName, Path explicitInstallDir, Path explicitOutputDir) {
    this.program = program;
    this.lookup = new Lookup(lookupPrefix());
    this.installDir = resolveInstallDir(explicitInstallDir);
    this.outputDir = createDir(resolveOutputDir(explicitOutputDir), "the output directory");
    this.classDir =
        scopeName == null ? outputDir : createDir(outputDir.resolve(scopeName), "the class dir");
    this.failureDir = createDir(outputDir.resolve("failure-snapshots"), "the failure snapshot dir");
    this.ports = PortBlocks.claim(outputDir, portBaseSeed(), program.portLayout());
    this.marker = resolveMarker();
    this.timeout = resolveTimeout();

    Map<String, String> assembled = new LinkedHashMap<>();
    assemble(assembled, installDir, classDir, ports, marker);
    this.vars = Collections.unmodifiableMap(assembled);
    this.baseEnv = Collections.unmodifiableMap(buildBaseEnv(this.vars));
  }

  // ---------------------------------------------------------------- what a subclass fills in

  /**
   * Puts every variable the program reads into {@code vars}, under the names the program knows them
   * by. Called once, during construction.
   *
   * <p>The only place in the harness that knows those names. A subclass may create directories it
   * needs below {@code classDir} with {@link #createDir}, and should report them back by overriding
   * {@link #workDir()}, {@link #stateDir()} and {@link #logDir()}.
   *
   * @param installDir the installation under test, empty when the program is already on {@code
   *     PATH}
   * @param classDir the directory this scope may write in
   * @param ports the block this scope owns, to be mapped onto whatever the program calls its ports
   * @param marker the command-line marker this scope's processes must carry, so that discovery
   *     finds them and nothing else
   */
  protected abstract void assemble(
      Map<String, String> vars,
      Optional<Path> installDir,
      Path classDir,
      PortBlock ports,
      String marker);

  /**
   * The system-property prefix the program's variables are looked up under, so that the properties
   * of one project cannot be mistaken for another's.
   */
  protected String lookupPrefix() {
    return "cats.";
  }

  /** The variable naming the installation to test. */
  protected String installDirVariable() {
    return "CATS_INSTALL_DIR";
  }

  /**
   * Whether the program can only be tested from an installation. False for a tool that is already
   * on {@code PATH}.
   */
  protected boolean requiresInstallDir() {
    return false;
  }

  /** Why a run without an installation cannot go on; a subclass adds how to get one. */
  protected String noInstallDirMessage() {
    return "There is no installation to test at " + installDirVariable();
  }

  /**
   * Checks that a resolved installation is one, for a subclass that knows what its own looks like.
   * The directory itself has already been checked to exist.
   */
  protected Path checkInstallDir(Path dir) {
    return dir;
  }

  /**
   * The prefix every marker of this harness' processes begins with, which is what tells them from
   * another project's on the same host.
   */
  protected String markerPrefix() {
    return "cats-";
  }

  /**
   * The variable-name prefixes worth echoing when the harness has to list what it defined — in the
   * hint on an unknown {@code ${NAME}}, say. Empty means every name.
   */
  public Set<String> echoedPrefixes() {
    return Set.of();
  }

  // ---------------------------------------------------------------- directories

  /** The installation under test, empty when the program is expected on {@code PATH} already. */
  public Optional<Path> installDir() {
    return installDir;
  }

  /** Where every scope of this run writes, and where the port blocks are claimed from. */
  public Path outputDir() {
    return outputDir;
  }

  /**
   * The directory this scope's program writes its own state into, wiped before the class runs.
   * Defaults to the whole class directory.
   */
  public Path workDir() {
    return classDir;
  }

  /** Where the program records the instances it starts, such as pid files. */
  public Path stateDir() {
    return workDir();
  }

  /** Where the program writes its logs. */
  public Path logDir() {
    return workDir();
  }

  /** Where a failing test's snapshot of the work directory and the transcript is copied. */
  public Path failureDir() {
    return failureDir;
  }

  // ---------------------------------------------------------------- the rest of the environment

  /** The program's variables only, for logging and for layering onto a session. */
  public Map<String, String> vars() {
    return vars;
  }

  /**
   * The inherited process environment plus the program's variables, with {@code PATH} prepended.
   */
  public Map<String, String> baseEnv() {
    return baseEnv;
  }

  /** The named traits of this run, as the program reads them off the environment it is given. */
  public Map<String, String> traits() {
    return program.traits(vars);
  }

  /** The ports this scope owns. */
  public PortBlock ports() {
    return ports;
  }

  /** The shape of that block, including which of its ports is the primary one. */
  public PortLayout portLayout() {
    return program.portLayout();
  }

  /** Where the instances a session starts are tracked, and from where a leak is finally killed. */
  public ProgramRegistry registry() {
    return registry;
  }

  /** The command-line marker this scope's processes carry, and that discovery is scoped to. */
  public String marker() {
    return marker;
  }

  /**
   * The marker prefix shared by every process this run's task has started, in this checkout.
   *
   * <p>Keyed on the output directory, which is per task and per checkout and stable across runs —
   * unlike the port, which is random per run. That is what lets a run find and kill a process an
   * earlier run of the same task left behind, without touching one belonging to another suite,
   * another checkout, or a developer's own installation.
   */
  public String taskMarkerPrefix() {
    return markerPrefix() + Integer.toHexString(outputDir.toString().hashCode()) + "-";
  }

  /**
   * The run-wide default timeout every command in every session starts from, overridable per
   * session with {@code cli.timeout(Duration)} and per call with the {@code run(Duration, line)}
   * overloads.
   */
  public Duration timeout() {
    return timeout;
  }

  /** Whether the given local port can be bound right now. */
  public static boolean isFree(int port) {
    try (ServerSocket s = new ServerSocket(port, 1, InetAddress.getLoopbackAddress())) {
      return s.getLocalPort() == port;
    } catch (IOException e) {
      return false;
    }
  }

  /** Creates a directory and every parent of it, or says which one it could not create. */
  protected static Path createDir(Path path, String label) {
    try {
      Files.createDirectories(path);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not create " + label + " at " + path, e);
    }
    return path;
  }

  /** The value of one of the program's variables, through the chain this project uses. */
  protected final String lookup(String name) {
    return lookup.get(name);
  }

  // ---------------------------------------------------------------- resolution

  private Optional<Path> resolveInstallDir(Path explicit) {
    Path dir = explicit;
    if (dir == null) {
      String configured = lookup.get(installDirVariable());
      if (configured == null) {
        if (requiresInstallDir()) {
          throw new IllegalStateException(noInstallDirMessage());
        }
        return Optional.empty();
      }
      dir = Path.of(configured);
    }
    dir = onDefaultFileSystem(dir);
    if (!Files.isDirectory(dir)) {
      throw new IllegalStateException(
          "There is no installation to test at " + installDirVariable() + "=" + dir);
    }
    return Optional.of(checkInstallDir(dir));
  }

  private Path resolveOutputDir(Path explicit) {
    if (explicit != null) {
      return onDefaultFileSystem(explicit);
    }
    String configured = Lookup.CATS.get("CATS_OUTPUT_DIR");
    return configured == null ? tempDir("cats-") : onDefaultFileSystem(Path.of(configured));
  }

  /**
   * Every directory the harness hands out is on the real filesystem: the commands it runs are real
   * processes with a real working directory, which no virtual path can stand in for.
   */
  private static Path onDefaultFileSystem(Path path) {
    return Path.of(path.toAbsolutePath().normalize().toString());
  }

  /**
   * The marker this scope's processes carry, derived from its own port — unique per class within a
   * run, and drawn from a base that is random per run, so two suites sharing a machine do not see
   * each other's processes. An explicit {@code CATS_MARKER} wins, for a CI job that would rather
   * name its own.
   */
  private String resolveMarker() {
    String explicit = Lookup.CATS.get("CATS_MARKER");
    return explicit == null ? taskMarkerPrefix() + ports.base() : explicit;
  }

  private static Duration resolveTimeout() {
    String configured = Lookup.CATS.get("CATS_TIMEOUT");
    return configured == null
        ? DEFAULT_TIMEOUT
        : Duration.ofSeconds(Long.parseLong(configured.trim()));
  }

  /**
   * Where this run's blocks start, from {@code CATS_PORT_BASE}, so that a failing run can be
   * replayed on the same ports. Random when unset.
   */
  private static int portBaseSeed() {
    String seed = Lookup.CATS.get("CATS_PORT_BASE");
    return seed == null ? -1 : Integer.parseInt(seed.trim());
  }

  private Map<String, String> buildBaseEnv(Map<String, String> programVars) {
    // Windows environment variable names are case-insensitive; a plain HashMap would happily hold
    // both "Path" and "PATH" and let the wrong one win.
    Map<String, String> env =
        isWindows() ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new LinkedHashMap<>();
    env.putAll(System.getenv());
    env.putAll(programVars);
    // With the launcher on PATH, one command string works on both shells: the bare launcher name
    // on bash and its .cmd twin on cmd, through PATHEXT rather than any rewriting by the harness.
    launcherDir()
        .ifPresent(
            dir -> {
              String path = env.get("PATH");
              env.put(
                  "PATH",
                  path == null || path.isBlank()
                      ? dir.toString()
                      : dir + System.getProperty("path.separator", ":") + path);
            });
    return env;
  }

  private Optional<Path> launcherDir() {
    Optional<Path> dir = installDir.flatMap(program::launcherDir);
    if (dir.isPresent() && !Files.isDirectory(dir.get())) {
      throw new IllegalStateException("The program's launcher directory is missing: " + dir.get());
    }
    return dir;
  }

  private static Path tempDir(String prefix) {
    try {
      return Files.createTempDirectory(prefix);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");
  }
}
