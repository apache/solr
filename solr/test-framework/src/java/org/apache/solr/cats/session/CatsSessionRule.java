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
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.solr.cats.EnabledOnShell;
import org.apache.solr.cats.ShellGate;
import org.apache.solr.cats.env.CatsHarnessEnv;
import org.apache.solr.cats.env.PortBlocks;
import org.apache.solr.cats.exec.CatsFiles;
import org.apache.solr.cats.exec.ProcessKill;
import org.apache.solr.cats.exec.ProgramUnderTest;
import org.apache.solr.cats.exec.RunningProcess;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The whole lifecycle of a shell test, in two scopes.
 *
 * <p>Per class: resolve the environment, check that nothing of ours is still running on the ports
 * we are about to claim, wipe the directory the program writes in, and open the class-scoped
 * session. Afterwards, stop what the class started and fail the class if anything outlived it.
 *
 * <p>Per test: a child of the class session, discarded afterwards together with whatever the test
 * started, and — when the test failed — a snapshot of the configured directories and the command
 * transcript under {@link CatsHarnessEnv#failureDir()}.
 *
 * <p>A rule rather than a pair of {@code @Before}/{@code @After} methods because only a rule learns
 * whether the test failed, and the snapshot has to be taken before the program is torn down. Either
 * scope works: as a {@code @ClassRule} the rule drives the class scope itself, and a base class
 * that would rather keep that in its own {@code @BeforeClass}/{@code @AfterClass} — where it is
 * ordered against the rest of the project's class setup — calls {@link #openClass} and {@link
 * #closeClass} from there.
 *
 * <p>Nothing here needs a program that has ports, state files or discovery: a program that declares
 * none gets the session, the transcript and the work directory, and the checks that do not apply
 * are skipped.
 *
 * <p>One rule drives exactly one {@link ProgramUnderTest}, through exactly one {@link
 * CatsHarnessEnv}: the program is consulted by the lifecycle here and never per command. A fixture
 * — a container, a served web root — is not a program under test and needs no rule of its own.
 *
 * <p>Two programs genuinely under test are two rules in one test class, each with its own
 * environment, program, port block, work directory, marker and leak check. The vocabulary's bare
 * {@code run()} is bound to the session the test case returns from {@code cli()}, and the other
 * program is addressed explicitly through its own rule ({@code other.session().run(...)}, {@code
 * other.env().ports().at(0)}). Two programs cost two port blocks and two work directories, which is
 * what they should cost, and their launcher directories never share one {@code PATH} — both
 * shipping a {@code bin/start} is not hypothetical.
 *
 * @param <E> the environment this project resolves, so a base class can hand its own type back
 */
public final class CatsSessionRule<E extends CatsHarnessEnv> implements TestRule {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** How long the post-flight check waits for the port block to come free. */
  private static final Duration LEAK_CHECK_TIMEOUT = Duration.ofSeconds(10);

  /** How long a process gets to die after being asked to. */
  private static final Duration KILL_TIMEOUT = Duration.ofSeconds(10);

  private final Function<Class<?>, E> envForClass;
  private final ProgramUnderTest program;
  private final List<Function<E, Path>> snapshotDirs;

  private E env;
  private CliSession classSession;
  private CliSession session;

  /**
   * @param envForClass resolves the environment for one test class
   * @param program what the sessions drive: how one of its processes is found, stopped and cleaned
   *     up after
   * @param snapshotDirs the directories a failing test's snapshot copies, each named after its own
   *     last path element
   */
  public CatsSessionRule(
      Function<Class<?>, E> envForClass,
      ProgramUnderTest program,
      List<Function<E, Path>> snapshotDirs) {
    this.envForClass = envForClass;
    this.program = program;
    this.snapshotDirs = List.copyOf(snapshotDirs);
  }

  // ---------------------------------------------------------------- class scope

  /** Opens the class scope with no project gate of its own. */
  public void openClass(Class<?> testClass) {
    openClass(testClass, e -> {});
  }

  /**
   * Resolves the environment, gives the class a clean work directory, and checks that our port
   * block is actually free before any test runs.
   *
   * @param gates project gates that need the environment — an assumption on one of its traits, say.
   *     Called once the environment is resolved and before anything is wiped or claimed, so a class
   *     this run skips costs nothing.
   */
  public void openClass(Class<?> testClass, Consumer<E> gates) {
    // Before anything else, so a class for the other shell costs nothing: no wipe, no port check,
    // and nothing started by a subclass' own class setup.
    ShellGate.requireShell(testClass.getAnnotation(EnabledOnShell.class));
    // Per class, not per JVM: directories and ports keyed to this class are what let classes run
    // concurrently in separate forks.
    env = envForClass.apply(testClass);
    gates.accept(env);
    // Order-independent: discovery goes through the running process list, not through files that
    // the wipe could destroy.
    preFlightLeakCheck();
    cleanWorkDir();
    classSession = CliSession.forScope(env, program, testClass.getSimpleName());
    if (log.isInfoEnabled()) {
      log.info(
          "CATS harness ready: install={} work={} ports={}",
          env.installDir().orElse(null),
          env.workDir(),
          env.ports());
    }
  }

  /**
   * Stops whatever the class started, then checks nothing was left running.
   *
   * <p>JUnit 4 runs {@code @AfterClass} even when {@code @BeforeClass} threw — an assumption
   * failure from {@link EnabledOnShell} included — so every step here tolerates a half-built or
   * entirely unbuilt harness.
   */
  public void closeClass() {
    List<String> problems = new ArrayList<>();
    try {
      if (classSession != null) {
        // close() stops the processes this class owns, one by one. No 'stop everything' command
        // first: it would walk the same per-class state directory, so every process would then be
        // stopped twice — a wasted subprocess and a misleading "failed to stop" warning for each.
        classSession.close();
      }
    } catch (RuntimeException | AssertionError e) {
      problems.add(e.toString());
    } finally {
      classSession = null;
    }
    try {
      if (env != null) {
        postFlightLeakCheck();
      }
    } catch (AssertionError e) {
      problems.add(e.toString());
    } finally {
      env = null;
    }
    if (!problems.isEmpty()) {
      throw new AssertionError("CATS harness teardown failed: " + String.join("; ", problems));
    }
  }

  /** The environment of the class being run, or null outside a class. */
  public E env() {
    return env;
  }

  /** The class-scoped session: what a class-scoped setup starts the program in. */
  public CliSession classSession() {
    return classSession;
  }

  /** The session for the running test, or null outside a test. */
  public CliSession session() {
    return session;
  }

  // ---------------------------------------------------------------- the rule

  @Override
  public Statement apply(Statement base, Description description) {
    return apply(base, description, s -> {});
  }

  /**
   * As {@link #apply(Statement, Description)}, telling {@code sink} which session the running test
   * got — for a base class that mirrors it in a field of its own, and is handed null once the test
   * is over.
   */
  public Statement apply(Statement base, Description description, Consumer<CliSession> sink) {
    return description.isSuite()
        ? classStatement(base, description)
        : testStatement(base, description, sink);
  }

  private Statement classStatement(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        openClass(description.getTestClass());
        try {
          base.evaluate();
        } finally {
          closeClass();
        }
      }
    };
  }

  private Statement testStatement(
      Statement base, Description description, Consumer<CliSession> sink) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        ShellGate.requireShell(description.getAnnotation(EnabledOnShell.class));
        session = classSession.child(description.getMethodName());
        sink.accept(session);
        // A test restricted to one shell may use that shell freely: there is no other shell left
        // for a pipe or a Unix tool to break on, so the portability rails step aside.
        if (description.getAnnotation(EnabledOnShell.class) != null
            || description.getTestClass().getAnnotation(EnabledOnShell.class) != null) {
          session.allowShellSpecific();
        }
        try {
          base.evaluate();
        } catch (Throwable failure) {
          saveOnFailure(description, failure);
          throw failure;
        } finally {
          try {
            session.close();
          } finally {
            session = null;
            sink.accept(null);
          }
        }
      }
    };
  }

  // ---------------------------------------------------------------- lifecycle internals

  private void cleanWorkDir() {
    try {
      Path workDir = env.workDir();
      if (Files.isDirectory(workDir)) {
        CatsFiles.rm(workDir);
      }
      Files.createDirectories(workDir);
      Files.createDirectories(env.logDir());
    } catch (IOException e) {
      throw new UncheckedIOException("Could not give the class a clean work directory", e);
    }
  }

  /**
   * Fails fast if our port block is in use, killing any process of ours that is somehow already on
   * it.
   *
   * <p>A port block is claimed once per run, so finding one of our own processes here means a
   * process from an earlier run outlived it — possible when a run was killed outright and the
   * shutdown hook never got to run.
   */
  private void preFlightLeakCheck() {
    // Processes carrying this task's marker but sitting outside this run's port span can only come
    // from an earlier run of the same task — the span is fixed when the run claims its first block,
    // from a base that is random per run. So this is safe against a sibling class in this run, and
    // against another suite or checkout, whose marker prefix differs.
    //
    // A process with no ports is left alone: there is nothing to measure it against, so it cannot
    // be told from one a sibling class of this run started.
    for (RunningProcess process : program.running(env.taskMarkerPrefix())) {
      if (process.ports().isEmpty()) {
        continue;
      }
      boolean thisRun =
          process.ports().stream()
              .anyMatch(
                  port -> PortBlocks.belongsToThisRun(env.outputDir(), port, program.portLayout()));
      if (!thisRun || ours(process)) {
        log.warn("Killing a process left behind by an earlier run of this task: {}", process);
        ProcessKill.killTree(process.pid(), KILL_TIMEOUT);
      }
    }

    if (program.portLayout().isEmpty()) {
      return;
    }
    List<Integer> busy = busyPorts();
    if (!busy.isEmpty()) {
      throw new AssertionError(
          "The port block is not free before the class started; in use: " + busy);
    }
  }

  /**
   * Fails the class if a process outlived it. Whatever is found is killed first.
   *
   * <p>Processes carrying our marker are checked for whatever the program is; the port block is
   * only checked for a program that declares a layout.
   */
  private void postFlightLeakCheck() {
    List<String> leaks = new ArrayList<>();

    // Only a running process is a leak. A state file left behind by one that is already gone is
    // litter, and not this check's business — the process list is what matters. A process with no
    // ports has nothing to filter on, and the marker it carries is this scope's own.
    for (RunningProcess process : program.running(env.marker())) {
      if (process.ports().isEmpty() || ours(process)) {
        leaks.add("still running: " + process);
        ProcessKill.killTree(process.pid(), KILL_TIMEOUT);
      }
    }

    if (!program.portLayout().isEmpty()) {
      long deadline = System.nanoTime() + LEAK_CHECK_TIMEOUT.toNanos();
      List<Integer> busy;
      do {
        busy = busyPorts();
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
    }

    if (!leaks.isEmpty()) {
      throw new AssertionError("Process leak after the test class: " + String.join("; ", leaks));
    }
  }

  /** Whether any of a process' ports is one this class claimed. */
  private boolean ours(RunningProcess process) {
    return process.ports().stream().anyMatch(port -> env.ports().contains(port));
  }

  private List<Integer> busyPorts() {
    List<Integer> busy = new ArrayList<>();
    for (int port : env.ports().all()) {
      if (!CatsHarnessEnv.isFree(port)) {
        busy.add(port);
      }
    }
    return busy;
  }

  private void saveOnFailure(Description description, Throwable failure) {
    if (failure instanceof AssumptionViolatedException) {
      return;
    }
    Path target =
        env.failureDir()
            .resolve(
                description.getTestClass().getSimpleName() + "-" + description.getMethodName());
    try {
      if (Files.exists(target)) {
        CatsFiles.rm(target);
      }
      Files.createDirectories(target);
      for (Function<E, Path> dir : snapshotDirs) {
        Path source = dir.apply(env);
        copyRecursively(source, target.resolve(source.getFileName().toString()));
      }
      Files.writeString(
          target.resolve("transcript.log"), session.transcript().text(), StandardCharsets.UTF_8);
      System.err.println("Snapshot of the failed test is at: " + target);
    } catch (IOException e) {
      log.warn("Could not write failure snapshot to {}", target, e);
    }
  }

  // ---------------------------------------------------------------- file utils

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
          // A running program may be rewriting files underneath us; a partial snapshot still helps.
          log.debug("Skipping {} in failure snapshot", path, e);
        }
      }
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
