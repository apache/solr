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

package org.apache.solr.shell.env;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import org.apache.solr.cli.SolrProcessManager;
import org.apache.solr.shell.Cli;

/**
 * The environment a shell test run operates in: where the Solr distribution lives, which {@code
 * SOLR_HOME} to use, which ports are ours, and the base environment handed to every spawned shell.
 *
 * <p>Resolved once per JVM. Each value is looked up in this order:
 *
 * <ol>
 *   <li>the process environment variable (how the Gradle task supplies it),
 *   <li>the system property {@code solr.cli.<lowercase.dotted.name>}, so an IDE run can override,
 *   <li>a fallback — a random free port block, or a temporary directory.
 * </ol>
 *
 * <p>{@code SOLR_TIP} has no fallback; without it there is nothing to test.
 */
public final class HarnessEnv {

  /** Prefix for the system-property form of a harness variable. */
  public static final String SYSPROP_PREFIX = "solr.cli.";

  private final Map<String, String> vars;
  private final Map<String, String> baseEnv;
  private final Ports ports;

  private HarnessEnv(Map<String, String> vars, Ports ports) {
    this.vars = Collections.unmodifiableMap(vars);
    this.ports = ports;
    this.baseEnv = Collections.unmodifiableMap(buildBaseEnv(vars));
  }

  /**
   * Builds an environment from explicit directories instead of the process environment, with a
   * fresh port block.
   *
   * <p>This is how the harness' own tests exercise the library without an unpacked Solr
   * distribution: {@code solrTip} only has to contain a {@code bin} directory. Suites that actually
   * drive {@code bin/solr} use {@link #forClass(String)}.
   */
  public static HarnessEnv of(Path solrTip, Path outputDir) {
    Path tip = requireDistribution(solrTip.toAbsolutePath().normalize());
    Path out = createDir(outputDir.toAbsolutePath().normalize(), "outputDir");
    Path solrHome = createDir(out.resolve("solr-home"), "SOLR_HOME");
    Ports ports = claimPorts(out);
    return new HarnessEnv(
        assemble(
            tip,
            out,
            solrHome,
            solrHome,
            createDir(solrHome.resolve("logs"), "SOLR_LOGS_DIR"),
            createDir(out.resolve("failure-snapshots"), "TEST_FAILURE_DIR"),
            ports,
            null),
        ports);
  }

  /**
   * Builds the environment for one test class: its own {@code SOLR_HOME}, {@code SOLR_PID_DIR},
   * logs and port block, all derived from the shared {@code TEST_OUTPUT_DIR}.
   *
   * <p>Everything a class can write to or listen on is keyed by {@code className}, so classes
   * running concurrently in separate forked JVMs cannot touch each other. That matters more than it
   * sounds: {@code SOLR_PID_DIR} is what scopes {@code solr stop --all}, and the class setup wipes
   * {@code SOLR_HOME} — sharing either across forks would have one class kill or delete another's
   * running node. Class names are unique within a run, so no coordination is needed for the
   * directories; only the ports need the cross-fork claim in {@link PortBlocks}.
   */
  public static HarnessEnv forClass(String className) {
    String solrTip = lookup("SOLR_TIP");
    if (solrTip == null || solrTip.isBlank()) {
      throw new IllegalStateException(
          "SOLR_TIP is not set, so there is no Solr distribution to test. Run "
              + "'./gradlew -p solr/packaging dev' and set SOLR_TIP (or -Dsolr.cli.solr.tip=...) "
              + "to the unpacked distribution, or run the tests via the 'shellTests' Gradle task.");
    }
    Path tip = requireDistribution(Path.of(solrTip).toAbsolutePath().normalize());
    Path out = dir("TEST_OUTPUT_DIR", () -> tempDir("solr-shell-tests"));
    Path classDir = createDir(out.resolve(className), "class output dir");
    Path solrHome = createDir(classDir.resolve("solr-home"), "SOLR_HOME");
    Ports ports = claimPorts(out);
    return new HarnessEnv(
        assemble(
            tip,
            out,
            solrHome,
            // Keeping the PID dir inside SOLR_HOME is what scopes 'solr stop --all' to this class,
            // and away from a developer's own Solr instance.
            dir("SOLR_PID_DIR", () -> solrHome),
            dir("SOLR_LOGS_DIR", () -> solrHome.resolve("logs")),
            dir("TEST_FAILURE_DIR", () -> out.resolve("failure-snapshots")),
            ports,
            lookup("SOLR_PICOCLI")),
        ports);
  }

  private static Map<String, String> assemble(
      Path tip,
      Path out,
      Path solrHome,
      Path pidDir,
      Path logsDir,
      Path failureDir,
      Ports ports,
      String picocli) {
    Map<String, String> vars = new LinkedHashMap<>();
    vars.put("SOLR_TIP", tip.toString());
    vars.put("TEST_OUTPUT_DIR", out.toString());
    vars.put("SOLR_HOME", solrHome.toString());
    vars.put("SOLR_PID_DIR", pidDir.toString());
    vars.put("SOLR_LOGS_DIR", logsDir.toString());
    vars.put("TEST_FAILURE_DIR", failureDir.toString());
    vars.put("SOLR_PORT", Integer.toString(ports.solr()));
    vars.put("SOLR_PORT_LISTEN", Integer.toString(ports.solr()));
    vars.put("SOLR2_PORT", Integer.toString(ports.solr2()));
    vars.put("SOLR3_PORT", Integer.toString(ports.solr3()));
    vars.put("ZK_PORT", Integer.toString(ports.zk()));
    // BATS' common_setup does the same; the checks are noisy and irrelevant under test.
    vars.put("SOLR_ULIMIT_CHECKS", "false");

    // Sandbox node discovery to the nodes this scope starts. Two channels are needed, because two
    // different JVMs have to agree:
    //
    //   SOLR_PROCESS_SCAN_MARKER  the CLI's own JVM. EnvUtils turns any SOLR_* variable into the
    //                             matching solr.* property, so SolrProcessManager reads it without
    //                             the harness having to touch SOLR_TOOL_OPTS — which some tests
    //                             legitimately set for themselves.
    //   SOLR_OPTS_INTERNAL        the node's command line, which is where the CLI looks for the
    //                             marker. bin/solr and bin/solr.cmd both append this to the node's
    //                             options, it is documented as internal, and no test sets it, so a
    //                             test overriding SOLR_OPTS cannot lose the marker.
    //
    // Without this, 'solr status' with no port reports every Solr on the host: a developer's own
    // node, or another suite sharing a CI machine.
    String marker = marker(ports);
    vars.put("SOLR_PROCESS_SCAN_MARKER", marker);
    vars.put("SOLR_OPTS_INTERNAL", "-D" + SolrProcessManager.PROCESS_SCAN_MARKER + "=" + marker);
    if (picocli != null && !picocli.isBlank()) {
      vars.put("SOLR_PICOCLI", picocli);
    }
    return vars;
  }

  /**
   * The command-line marker this scope's nodes carry.
   *
   * <p>Derived from the scope's own port, which is unique per class within a run and drawn from a
   * random base per run — so two suites sharing a machine do not see each other's nodes. An
   * explicit {@code SOLR_PROCESS_SCAN_MARKER} wins, for a CI job that would rather name its own.
   */
  private static String marker(Ports ports) {
    String explicit = lookup("SOLR_PROCESS_SCAN_MARKER");
    return (explicit == null || explicit.isBlank()) ? "solr-shell-test-" + ports.solr() : explicit;
  }

  private static Path requireDistribution(Path tip) {
    if (!Files.isDirectory(tip.resolve("bin"))) {
      throw new IllegalStateException(
          "SOLR_TIP=" + tip + " does not look like a Solr distribution (no bin/ directory)");
    }
    return tip;
  }

  /**
   * One block per caller, claimed through {@code outputDir} so concurrent forks cannot overlap.
   * {@code SOLR_PORT} in the environment pins the block instead, for reproducing one class by hand.
   */
  private static Ports claimPorts(Path outputDir) {
    String explicit = lookup("SOLR_PORT");
    if (explicit != null && !explicit.isBlank()) {
      return Ports.fromBase(Integer.parseInt(explicit.trim()));
    }
    String seed = lookup("SOLR_PORT_BASE");
    int seedBase = (seed == null || seed.isBlank()) ? -1 : Integer.parseInt(seed.trim());
    return PortBlocks.claim(outputDir, seedBase);
  }

  /** Whether the given local port can be bound right now. */
  public static boolean isFree(int port) {
    try (ServerSocket s = new ServerSocket(port, 1, InetAddress.getLoopbackAddress())) {
      return s.getLocalPort() == port;
    } catch (IOException e) {
      return false;
    }
  }

  private static Path dir(String name, Supplier<Path> fallback) {
    String value = lookup(name);
    Path path = (value == null || value.isBlank()) ? fallback.get() : Path.of(value);
    return createDir(path.toAbsolutePath().normalize(), name);
  }

  private static Path createDir(Path path, String label) {
    try {
      Files.createDirectories(path);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not create " + label + " at " + path, e);
    }
    return path;
  }

  private static Path tempDir(String prefix) {
    try {
      return Files.createTempDirectory(prefix);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Environment variable, then {@code solr.cli.<lowercase.dotted>} system property. */
  private static String lookup(String name) {
    String fromEnv = System.getenv(name);
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv;
    }
    return System.getProperty(SYSPROP_PREFIX + name.toLowerCase(Locale.ROOT).replace('_', '.'));
  }

  private static Map<String, String> buildBaseEnv(Map<String, String> harnessVars) {
    // Windows environment variable names are case-insensitive; a plain HashMap would happily hold
    // both "Path" and "PATH" and let the wrong one win.
    Map<String, String> env =
        isWindows() ? new TreeMap<>(String.CASE_INSENSITIVE_ORDER) : new LinkedHashMap<>();
    env.putAll(System.getenv());
    env.putAll(harnessVars);
    // 'solr' on bash and 'solr.cmd' on cmd (via PATHEXT) both resolve from here, so one command
    // string works on both shells.
    String binDir = Path.of(harnessVars.get("SOLR_TIP"), "bin").toString();
    String path = env.get("PATH");
    env.put(
        "PATH",
        path == null || path.isBlank()
            ? binDir
            : binDir + System.getProperty("path.separator", ":") + path);
    return env;
  }

  private static boolean isWindows() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");
  }

  /** The harness-specific variables only, for logging and for layering onto a session. */
  public Map<String, String> vars() {
    return vars;
  }

  /** The inherited process environment plus the harness variables, with {@code PATH} prepended. */
  public Map<String, String> baseEnv() {
    return baseEnv;
  }

  public Ports ports() {
    return ports;
  }

  public Path solrTip() {
    return Path.of(vars.get("SOLR_TIP"));
  }

  public Path solrHome() {
    return Path.of(vars.get("SOLR_HOME"));
  }

  public Path pidDir() {
    return Path.of(vars.get("SOLR_PID_DIR"));
  }

  public Path logsDir() {
    return Path.of(vars.get("SOLR_LOGS_DIR"));
  }

  public Path testOutputDir() {
    return Path.of(vars.get("TEST_OUTPUT_DIR"));
  }

  public Path failureDir() {
    return Path.of(vars.get("TEST_FAILURE_DIR"));
  }

  /**
   * Which command-line implementation {@code bin/solr} will dispatch to for this run.
   *
   * <p>The default tracks {@code SolrCLI.main}, which reads {@code solr.picocli} with a default of
   * false. When that flips to picocli, this flips with it — it is the one place the harness encodes
   * the default.
   */
  public Cli cli() {
    return Boolean.parseBoolean(vars.getOrDefault("SOLR_PICOCLI", "false"))
        ? Cli.PICOCLI
        : Cli.COMMONS_CLI;
  }
}
