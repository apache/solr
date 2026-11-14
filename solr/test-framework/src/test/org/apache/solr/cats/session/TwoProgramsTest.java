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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.solr.cats.CatsDsl;
import org.apache.solr.cats.env.CatsHarnessEnv;
import org.apache.solr.cats.env.PortBlock;
import org.apache.solr.cats.env.PortLayout;
import org.apache.solr.cats.exec.BackgroundCommand;
import org.apache.solr.cats.exec.ProgramUnderTest;
import org.apache.solr.cats.exec.Shells;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Two programs under test in one class: two {@link CatsSessionRule}s, each with its own program,
 * environment, port block, work directory, marker and leak check.
 *
 * <p>Exactly one program per environment is the invariant, so a second program is a second rule and
 * never a second argument. The bare {@code run()} of the vocabulary goes to the session {@link
 * #cli()} returns; the other program is addressed explicitly through its own rule.
 */
public class TwoProgramsTest implements CatsDsl {

  /** Equal strides, so blocks claimed from the one counter stay disjoint whatever the offsets. */
  private static final PortLayout TOOL_PORTS =
      new PortLayout(4, new int[] {0, 1}, 20_000, 30_000, 200, 0);

  private static final PortLayout OTHER_PORTS =
      new PortLayout(4, new int[] {0}, 20_000, 30_000, 200, 0);

  private static final ProgramUnderTest TOOL =
      new ProgramUnderTest() {
        @Override
        public PortLayout portLayout() {
          return TOOL_PORTS;
        }
      };

  private static final ProgramUnderTest OTHER =
      new ProgramUnderTest() {
        @Override
        public PortLayout portLayout() {
          return OTHER_PORTS;
        }
      };

  private static final Path TMP = tempDir();
  private static final Path TOOL_INSTALL = install("mytool", TMP.resolve("tool-install"));
  private static final Path OTHER_INSTALL = install("othertool", TMP.resolve("other-install"));

  /** One output directory, which is what makes the two blocks come from the same counter. */
  private static final Path OUTPUT = makeDir(TMP.resolve("out"));

  @ClassRule
  public static final CatsSessionRule<TestEnv> tool = harness(TOOL, TOOL_INSTALL, "tool");

  @ClassRule
  public static final CatsSessionRule<TestEnv> other = harness(OTHER, OTHER_INSTALL, "other");

  private CliSession toolCli;
  private CliSession otherCli;

  @Rule
  public final TestRule toolSession =
      (base, description) -> tool.apply(base, description, session -> toolCli = session);

  @Rule
  public final TestRule otherSession =
      (base, description) -> other.apply(base, description, session -> otherCli = session);

  @Override
  public CliSession cli() {
    return toolCli;
  }

  @Test
  public void theTwoProgramsGetDisjointPortBlocks() {
    int[] toolPorts = tool.env().ports().all();
    int[] otherPorts = other.env().ports().all();
    assertNotEquals(tool.env().ports().base(), other.env().ports().base());
    for (int port : otherPorts) {
      assertFalse(
          Arrays.toString(toolPorts) + " and " + Arrays.toString(otherPorts) + " share " + port,
          tool.env().ports().contains(port));
    }
  }

  @Test
  public void eachProgramGetsItsOwnMarkerAndWorkDirectory() {
    assertNotEquals(tool.env().marker(), other.env().marker());
    assertNotEquals(tool.env().workDir(), other.env().workDir());
  }

  @Test
  public void theBareRunGoesToTheProgramTheTestCaseReturns() {
    run("mytool echo first").assertExact("first");
    // Two launcher directories never share one PATH: the other program's launcher is not on this
    // session's.
    runFail("othertool echo first");

    otherCli.run("othertool echo second").assertExact("second");
    otherCli.runFail("mytool echo second");
  }

  /**
   * Each rule stops what its own program started, and leaves the other program's alone.
   *
   * <p>Driven by hand on a pair of rules of its own: the class' own rules close after the last
   * test, which is too late for a test to assert on.
   */
  @Test
  public void eachTeardownStopsOnlyItsOwnProcess() {
    CatsSessionRule<TestEnv> firstRule = harness(TOOL, TOOL_INSTALL, "teardown-tool");
    CatsSessionRule<TestEnv> secondRule = harness(OTHER, OTHER_INSTALL, "teardown-other");
    firstRule.openClass(getClass());
    secondRule.openClass(getClass());

    BackgroundCommand first = firstRule.classSession().runInBackground("mytool work 60 0");
    BackgroundCommand second = secondRule.classSession().runInBackground("othertool work 60");
    first.awaitOutput("starting", Duration.ofSeconds(60));
    second.awaitOutput("starting", Duration.ofSeconds(60));

    firstRule.closeClass();
    assertFalse("its own process is stopped", alive(first.pid()));
    assertTrue("the other program's process is untouched", alive(second.pid()));

    secondRule.closeClass();
    assertFalse(alive(second.pid()));
  }

  private static CatsSessionRule<TestEnv> harness(
      ProgramUnderTest program, Path installDir, String scopeSuffix) {
    return new CatsSessionRule<>(
        testClass ->
            new TestEnv(program, testClass.getSimpleName() + "-" + scopeSuffix, installDir),
        program,
        List.<Function<TestEnv, Path>>of(CatsHarnessEnv::workDir));
  }

  private static boolean alive(long pid) {
    return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
  }

  /** One environment per program, each writing in a directory keyed to its own scope name. */
  private static final class TestEnv extends CatsHarnessEnv {

    TestEnv(ProgramUnderTest program, String scopeName, Path installDir) {
      super(program, scopeName, installDir, OUTPUT);
    }

    @Override
    protected void assemble(
        Map<String, String> vars,
        Optional<Path> installDir,
        Path classDir,
        PortBlock ports,
        String marker) {
      // Nothing to assemble: both stand-ins are driven by their arguments alone.
    }
  }

  private static Path tempDir() {
    try {
      return Files.createTempDirectory("cats-two-programs");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Path makeDir(Path dir) {
    try {
      return Files.createDirectories(dir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Installs one of the stand-in programs from {@code src/test-files/cats} into an installation of
   * its own, under the name this platform's shell resolves from the bare word.
   */
  private static Path install(String name, Path installDir) {
    boolean cmd = "cmd".equals(Shells.current().id());
    String fixture = name + (cmd ? ".cmd" : ".sh");
    try {
      Path launcherDir = Files.createDirectories(installDir.resolve("bin"));
      Path installed = launcherDir.resolve(cmd ? name + ".cmd" : name);
      try (InputStream in = TwoProgramsTest.class.getResourceAsStream("/cats/" + fixture)) {
        if (in == null) {
          throw new IllegalStateException(fixture + " must be on the test classpath");
        }
        Files.copy(in, installed);
      }
      if (!cmd) {
        Files.setPosixFilePermissions(installed, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
      return installDir;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
