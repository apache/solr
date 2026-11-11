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

package org.apache.solr.shell;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.solr.SolrTestCase;
import org.apache.solr.shell.env.HarnessEnv;
import org.apache.solr.shell.env.Ports;
import org.apache.solr.shell.env.ProcessScopeLock;
import org.apache.solr.shell.exec.CommandResult;
import org.apache.solr.shell.exec.ProcessKill;
import org.apache.solr.shell.exec.Shell;
import org.apache.solr.shell.exec.SolrProcesses;
import org.apache.solr.shell.session.CliSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for tests that drive Solr's start scripts the way the BATS suites do, with {@code
 * run}, {@code status}, {@code output}, reusable helpers and automatic cleanup — but as one test
 * that can run against {@code bin/solr} under bash and (eventually) {@code bin\solr.cmd} under
 * cmd.exe.
 *
 * <p>Two scopes, mirroring BATS:
 *
 * <ul>
 *   <li>{@link #classCli} is the class-scoped session, BATS' {@code setup_file}. A subclass can
 *       start a long-lived Solr from its own {@code @BeforeClass}; this class's
 *       {@code @BeforeClass} has already run by then, and the node is stopped automatically after
 *       the class.
 *   <li>{@link #cli} is the per-test session, BATS' {@code setup}. Environment and cwd changes a
 *       test makes are discarded afterwards, and any Solr it started is stopped after it.
 * </ul>
 *
 * <p>On failure, {@code SOLR_HOME} and the command transcript are copied to {@code
 * $TEST_FAILURE_DIR/<Class>-<method>/} and the path is printed, which is BATS' {@code
 * save_home_on_failure}.
 */
public abstract class SolrShellTestCase extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Pattern PID_FILE = Pattern.compile("solr-(\\d+)\\.pid");

  /** How long the post-flight check waits for the port block to come free. */
  private static final Duration LEAK_CHECK_TIMEOUT = Duration.ofSeconds(10);

  /** The resolved environment: distribution, {@code SOLR_HOME}, ports, base environment. */
  protected static HarnessEnv harnessEnv;

  /** The class-scoped session. Solr started here lives for the whole class. */
  protected static CliSession classCli;

  /** Shared for an ordinary class, exclusive for a {@link GlobalProcessScope} one. */
  private static ProcessScopeLock processScopeLock;

  /** The session for the running test. */
  protected CliSession cli;

  /**
   * Resolves the environment, gives the class a clean {@code SOLR_HOME}, and checks that our port
   * block is actually free before any test runs.
   */
  @BeforeClass
  public static void setUpShellHarness() throws Exception {
    // Before anything else, so a class for the other shell costs nothing: no SOLR_HOME wipe, no
    // port check, and no Solr started by a subclass @BeforeClass.
    requireShell(getTestClass().getAnnotation(EnabledOnShell.class));
    // Per class, not per JVM: directories and ports keyed to this class are what let classes run
    // concurrently in separate forks.
    harnessEnv = HarnessEnv.forClass(getTestClass().getSimpleName());
    // Taken before anything starts, and released only after this class's nodes are stopped, so a
    // @GlobalProcessScope class really does see an otherwise idle machine.
    boolean wantsWholeMachine = getTestClass().getAnnotation(GlobalProcessScope.class) != null;
    processScopeLock = ProcessScopeLock.acquire(harnessEnv.testOutputDir(), wantsWholeMachine);
    if (wantsWholeMachine) {
      requireNoForeignSolr();
    }
    CliSession.resetKnownNodes();
    cleanSolrHome();
    preFlightLeakCheck();
    classCli = CliSession.forScope(harnessEnv, getTestClass().getSimpleName());
    if (log.isInfoEnabled()) {
      log.info(
          "Shell harness ready: SOLR_TIP={} SOLR_HOME={} ports={}",
          harnessEnv.solrTip(),
          harnessEnv.solrHome(),
          harnessEnv.ports());
    }
  }

  /**
   * Stops whatever the class started, checks nothing was left running, and only then releases the
   * process-scope lock.
   *
   * <p>JUnit 4 runs {@code @AfterClass} even when {@code @BeforeClass} threw — an assumption
   * failure from {@link EnabledOnShell} included — so every step here tolerates a half-built or
   * entirely unbuilt harness.
   */
  @AfterClass
  public static void tearDownShellHarness() {
    List<String> problems = new ArrayList<>();
    try {
      if (classCli != null) {
        classCli.shutdownAll();
        classCli.close();
      }
    } catch (RuntimeException | AssertionError e) {
      problems.add(e.toString());
    } finally {
      classCli = null;
    }
    try {
      if (harnessEnv != null) {
        postFlightLeakCheck();
      }
    } catch (AssertionError e) {
      problems.add(e.toString());
    } finally {
      // Last, so no other class can claim the machine until this one's nodes are gone.
      if (processScopeLock != null) {
        processScopeLock.close();
        processScopeLock = null;
      }
      harnessEnv = null;
    }
    if (!problems.isEmpty()) {
      throw new AssertionError("Shell harness teardown failed: " + String.join("; ", problems));
    }
  }

  /**
   * Creates the per-test session, snapshots {@code SOLR_HOME} if the test failed, and stops
   * whatever the test started.
   *
   * <p>This is a rule rather than {@code @Before}/{@code @After} because JUnit 4 only tells a rule
   * whether the test failed, and the snapshot has to happen before the harness tears the node down.
   */
  @Rule
  public final TestRule shellSessionRule =
      (base, description) ->
          new Statement() {
            @Override
            public void evaluate() throws Throwable {
              requireShell(description.getAnnotation(EnabledOnShell.class));
              cli = classCli.child(description.getMethodName());
              try {
                base.evaluate();
              } catch (Throwable failure) {
                saveHomeOnFailure(description, failure);
                throw failure;
              } finally {
                try {
                  cli.close();
                } finally {
                  cli = null;
                }
              }
            }
          };

  /** The ports this run owns. */
  protected Ports ports() {
    return harnessEnv.ports();
  }

  /**
   * Skips a {@link GlobalProcessScope} class when a Solr the harness did not start is running.
   *
   * <p>The process-scope lock only coordinates this run's own forks; it says nothing about the
   * machine. A developer with Solr up on 8983 would otherwise see these tests fail for a reason
   * that is not their change — and killing their node is not ours to do. So we skip, and say why.
   *
   * <p>Only {@code @GlobalProcessScope} classes care. An ordinary class has its own random port
   * block and its own {@code SOLR_PID_DIR}, so a foreign node is invisible to it.
   */
  private static void requireNoForeignSolr() {
    List<Integer> foreign = SolrProcesses.listeningPorts();
    assumeTrue(
        "Needs a machine with no other Solr running, but found Solr on port(s) "
            + foreign
            + ". Stop them, or run this class on its own.",
        foreign.isEmpty());
  }

  /** Skips the test, or the class, when {@link EnabledOnShell} does not name the current shell. */
  private static void requireShell(EnabledOnShell restriction) {
    if (restriction == null) {
      return;
    }
    Shell current = Shell.defaultShell();
    List<Shell> allowed = List.of(restriction.value());
    assumeTrue("Only runs on " + allowed + "; this run uses " + current, allowed.contains(current));
  }

  // ------------------------------------------------- method-scope shorthands
  // These all delegate to {@link #cli}, so a test method reads much like its BATS original:
  // run("solr zk ls /"). Only {@code @BeforeClass} and {@code @AfterClass} have to name a session,
  // and there the session is {@link #classCli} — which keeps setup_file scope visible.

  protected CommandResult run(String line, Object... fmtArgs) {
    return cli.run(line, fmtArgs);
  }

  protected CommandResult run(Duration commandTimeout, String line, Object... fmtArgs) {
    return cli.run(commandTimeout, line, fmtArgs);
  }

  protected CommandResult run(List<String> argv) {
    return cli.run(argv);
  }

  protected CommandResult runSeparated(String line, Object... fmtArgs) {
    return cli.runSeparated(line, fmtArgs);
  }

  protected CommandResult script(String bashScript, String cmdScript) {
    return cli.script(bashScript, cmdScript);
  }

  protected CommandResult lastResult() {
    return cli.lastResult();
  }

  protected CliSession env(String key, String value) {
    return cli.env(key, value);
  }

  protected String env(String key) {
    return cli.env(key);
  }

  protected CliSession cd(String dir) {
    return cli.cd(dir);
  }

  protected CliSession cd(Path dir) {
    return cli.cd(dir);
  }

  protected Path cwd() {
    return cli.cwd();
  }

  protected Path workDir() {
    return cli.workDir();
  }

  protected boolean waitFor(Duration waitTimeout, Duration interval, BooleanSupplier condition) {
    return cli.waitFor(waitTimeout, interval, condition);
  }

  protected void awaitOrFail(
      String what, Duration waitTimeout, Duration interval, BooleanSupplier condition) {
    cli.awaitOrFail(what, waitTimeout, interval, condition);
  }

  protected CliSession.HttpResult http(String pathAndQuery) {
    return cli.http(pathAndQuery);
  }

  protected CliSession.HttpResult http(int port, String pathAndQuery) {
    return cli.http(port, pathAndQuery);
  }

  protected boolean collectionExists(String collection) {
    return cli.collectionExists(collection);
  }

  protected boolean configExists(String configName) {
    return cli.configExists(configName);
  }

  protected void deleteAllCollections() {
    cli.deleteAllCollections();
  }

  protected void shutdownAll() {
    cli.shutdownAll();
  }

  protected void dumpSolrLog() {
    cli.dumpSolrLog();
  }

  // ---------------------------------------------------------------- lifecycle internals

  private static void cleanSolrHome() throws IOException {
    Path solrHome = harnessEnv.solrHome();
    if (Files.isDirectory(solrHome)) {
      deleteRecursively(solrHome);
    }
    Files.createDirectories(solrHome);
    Files.createDirectories(harnessEnv.logsDir());
  }

  /**
   * Fails fast if our port block is in use, and clears away any {@code solr-*.pid} file a killed
   * earlier run left behind.
   */
  private static void preFlightLeakCheck() throws IOException {
    Path pidDir = harnessEnv.pidDir();
    if (Files.isDirectory(pidDir)) {
      try (Stream<Path> files = Files.list(pidDir)) {
        for (Path file : files.toList()) {
          Matcher m = PID_FILE.matcher(file.getFileName().toString());
          if (!m.matches()) {
            continue;
          }
          log.warn("Stale Solr pid file from an earlier run: {}; killing and removing", file);
          String text = Files.readString(file, StandardCharsets.UTF_8).trim();
          if (!text.isEmpty()) {
            try {
              ProcessKill.killTree(Long.parseLong(text), Duration.ofSeconds(10));
            } catch (NumberFormatException e) {
              log.warn("Unreadable pid in {}", file);
            }
          }
          Files.deleteIfExists(file);
        }
      }
    }

    List<Integer> busy = new ArrayList<>();
    for (int port : harnessEnv.ports().all()) {
      if (!HarnessEnv.isFree(port)) {
        busy.add(port);
      }
    }
    if (!busy.isEmpty()) {
      throw new AssertionError(
          "Shell harness port block is not free before the class started; in use: " + busy);
    }
  }

  /** Fails the class if a Solr node outlived it. Whatever is found is killed first. */
  private static void postFlightLeakCheck() {
    List<String> leaks = new ArrayList<>();

    Path pidDir = harnessEnv.pidDir();
    if (Files.isDirectory(pidDir)) {
      try (Stream<Path> files = Files.list(pidDir)) {
        for (Path file : files.toList()) {
          if (PID_FILE.matcher(file.getFileName().toString()).matches()) {
            leaks.add("leftover pid file " + file);
            try {
              String text = Files.readString(file, StandardCharsets.UTF_8).trim();
              if (!text.isEmpty()) {
                ProcessKill.killTree(Long.parseLong(text), Duration.ofSeconds(10));
              }
              Files.deleteIfExists(file);
            } catch (IOException | NumberFormatException e) {
              log.warn("Could not clear {}", file, e);
            }
          }
        }
      } catch (IOException e) {
        log.warn("Could not scan {}", pidDir, e);
      }
    }

    long deadline = System.nanoTime() + LEAK_CHECK_TIMEOUT.toNanos();
    List<Integer> busy;
    do {
      busy = new ArrayList<>();
      for (int port : harnessEnv.ports().all()) {
        if (!HarnessEnv.isFree(port)) {
          busy.add(port);
        }
      }
      if (busy.isEmpty()) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    } while (System.nanoTime() < deadline);

    if (!busy.isEmpty()) {
      leaks.add("ports still in use: " + busy);
    }
    if (!leaks.isEmpty()) {
      throw new AssertionError("Process leak after the test class: " + String.join("; ", leaks));
    }
  }

  private void saveHomeOnFailure(Description description, Throwable failure) {
    if (failure instanceof org.junit.internal.AssumptionViolatedException) {
      return;
    }
    Path target =
        harnessEnv
            .failureDir()
            .resolve(
                description.getTestClass().getSimpleName() + "-" + description.getMethodName());
    try {
      if (Files.exists(target)) {
        deleteRecursively(target);
      }
      Files.createDirectories(target);
      copyRecursively(harnessEnv.solrHome(), target.resolve("solr-home"));
      Files.writeString(
          target.resolve("transcript.log"), cli.transcript().text(), StandardCharsets.UTF_8);
      System.err.println("SOLR_HOME snapshot for the failed test is at: " + target);
    } catch (IOException e) {
      log.warn("Could not write failure snapshot to {}", target, e);
    }
  }

  // ---------------------------------------------------------------- file utils

  private static void deleteRecursively(Path root) throws IOException {
    Files.walkFileTree(
        root,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private static void copyRecursively(Path source, Path target) throws IOException {
    if (!Files.exists(source)) {
      return;
    }
    try (Stream<Path> paths = Files.walk(source)) {
      for (Path path : paths.toList()) {
        Path destination = target.resolve(source.relativize(path).toString());
        try {
          if (Files.isDirectory(path)) {
            Files.createDirectories(destination);
          } else {
            Files.createDirectories(destination.getParent());
            Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
          }
        } catch (IOException e) {
          // A running Solr may be rewriting files underneath us; a partial snapshot still helps.
          log.debug("Skipping {} in failure snapshot", path, e);
        }
      }
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
