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

import static org.apache.solr.cats.EnabledOnShell.BASH;
import static org.apache.solr.cats.EnabledOnShell.CMD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpHeaders;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.solr.cats.EnabledOnShell;
import org.apache.solr.cats.ShellGate;
import org.apache.solr.cats.env.CatsHarnessEnv;
import org.apache.solr.cats.env.PortBlock;
import org.apache.solr.cats.env.PortLayout;
import org.apache.solr.cats.exec.BackgroundCommand;
import org.apache.solr.cats.exec.CatsFiles;
import org.apache.solr.cats.exec.CommandResult;
import org.apache.solr.cats.exec.ProgramUnderTest;
import org.apache.solr.cats.exec.RunningProcess;
import org.apache.solr.cats.exec.Shells;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit tests for the harness itself — {@code env}, {@code cd}, {@code run}, {@code runWithStdin},
 * variable resolution, quoting, timeouts.
 *
 * <p>Deliberately uses shell built-ins and the stand-in program under {@code src/test-files/cats},
 * so it needs nothing installed and runs in a couple of seconds. It runs on whichever shell the
 * platform uses, which is how the {@code Shells.CMD} half of the harness gets covered at all; the
 * handful of cases that really are one shell's carry {@link EnabledOnShell}.
 */
public class CliSessionTest {

  /** Makes {@link EnabledOnShell} effective. */
  @Rule public final ShellGate shellGate = new ShellGate();

  @Rule public final TestName testName = new TestName();

  private Path tmp;
  private TestEnv env;
  private CliSession session;

  /**
   * A program with nothing overridden but its ports and discovery, which the ownership cases
   * script. Everything else here needs no program at all, which is the point of every method having
   * a default.
   */
  private final ScriptedProgram program = new ScriptedProgram();

  private static final PortLayout LAYOUT =
      new PortLayout(4, new int[] {0, 1, 2, 1000}, 20_000, 30_000, 200, 0);

  private static final class ScriptedProgram implements ProgramUnderTest {
    private List<RunningProcess> running = List.of();

    @Override
    public PortLayout portLayout() {
      return LAYOUT;
    }

    @Override
    public List<RunningProcess> running(String marker) {
      return running;
    }
  }

  /** The least a project has to supply: the variables its program reads. */
  private static final class TestEnv extends CatsHarnessEnv {

    TestEnv(ProgramUnderTest program, Path installDir, Path outputDir) {
      super(program, null, installDir, outputDir);
    }

    @Override
    protected void assemble(
        Map<String, String> vars,
        Optional<Path> installDir,
        Path classDir,
        PortBlock ports,
        String marker) {
      vars.put("MYTOOL_HOME", createDir(classDir.resolve("home"), "MYTOOL_HOME").toString());
      vars.put("MYTOOL_PORT", Integer.toString(ports.at(0)));
      vars.put("MYTOOL2_PORT", Integer.toString(ports.at(1)));
      vars.put("MYTOOL3_PORT", Integer.toString(ports.at(2)));
      vars.put("MYTOOL_ADMIN_PORT", Integer.toString(ports.at(3)));
    }

    @Override
    public Path workDir() {
      return Path.of(vars().get("MYTOOL_HOME"));
    }

    @Override
    public Set<String> echoedPrefixes() {
      return Set.of("MYTOOL", "TEST_");
    }
  }

  @Before
  public void openSession() throws IOException {
    tmp = Files.createTempDirectory("cats-session-test");
    Path launcherDir = Files.createDirectories(tmp.resolve("install").resolve("bin"));
    installStandInProgram(launcherDir);
    env = new TestEnv(program, launcherDir.getParent(), Files.createDirectory(tmp.resolve("out")));
    session = CliSession.forScope(env, program, testName.getMethodName());
  }

  /**
   * Installs the stand-in program from {@code src/test-files/cats} in the launcher directory, under
   * the name this platform's shell resolves from the bare word {@code mytool}.
   */
  private static void installStandInProgram(Path launcherDir) throws IOException {
    boolean cmd = "cmd".equals(Shells.current().id());
    String fixture = cmd ? "mytool.cmd" : "mytool.sh";
    Path installed = launcherDir.resolve(cmd ? "mytool.cmd" : "mytool");
    try (InputStream in = CliSessionTest.class.getResourceAsStream("/cats/" + fixture)) {
      assertNotNull(fixture + " must be on the test classpath", in);
      Files.copy(in, installed);
    }
    if (!cmd) {
      Files.setPosixFilePermissions(installed, PosixFilePermissions.fromString("rwxr-xr-x"));
    }
  }

  @After
  public void closeSession() throws IOException {
    if (session != null) {
      session.close();
    }
    if (tmp != null) {
      CatsFiles.rm(tmp);
    }
  }

  @Test
  @EnabledOnShell(BASH) // Reading the environment back needs the shell's own spelling.
  public void envReachesTheShell() {
    session.env("MARKER", "hello world");
    session.allowShellSpecific();
    session.run("printenv MARKER").assertExitCode(0).assertExact("hello world");
  }

  @Test
  public void envDoesNotLeakBetweenSessions() {
    session.env("MARKER", "set-on-the-parent");
    CliSession child = session.child("child");
    try {
      assertEquals("set-on-the-parent", child.env("MARKER"));
      child.env("MARKER", "set-on-the-child");
      assertEquals("set-on-the-child", child.env("MARKER"));
    } finally {
      child.close();
    }
    assertEquals(
        "a child's change must not reach its parent", "set-on-the-parent", session.env("MARKER"));
  }

  @Test
  public void harnessVariablesArePresent() {
    session.run("echo ${MYTOOL_HOME}").assertExitCode(0).assertExact(env.workDir().toString());
  }

  @Test
  public void runResolvesEveryVariableSpelling() {
    // All three mean "the environment variable", resolved by the harness before any shell sees it.
    String admin = String.valueOf(env.ports().at(3));
    session.run("echo ${MYTOOL_ADMIN_PORT}").assertExact(admin);
    session.run("echo $MYTOOL_ADMIN_PORT").assertExact(admin);
    session.run("echo {{MYTOOL_ADMIN_PORT}}").assertExact(admin);
  }

  @Test
  public void assertionsResolveVariablesToo() {
    // The same spelling means the same thing in an assertion as in a command line, so the two
    // cannot drift apart.
    session.run("echo port-${MYTOOL_ADMIN_PORT}").assertContains("port-${MYTOOL_ADMIN_PORT}");
    session.run("echo port-${MYTOOL_ADMIN_PORT}").assertExact("port-${MYTOOL_ADMIN_PORT}");
    session.run("echo nothing").assertNotContains("port-${MYTOOL_ADMIN_PORT}");
  }

  @Test
  public void anUnknownVariableInAnAssertionFailsToo() {
    AssertionError failure =
        assertThrows(
            AssertionError.class, () -> session.run("echo hi").assertContains("${NO_SUCH_VAR}"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("Unknown variable"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("an assertion"));
  }

  @Test
  public void runResolvesVariablesTheTestSetItself() {
    session.env("MARKER", "from-the-test");
    session.run("echo ${MARKER}").assertExact("from-the-test");
  }

  @Test
  public void runResolvesVariablesInTheArgvForm() {
    session
        .run(List.of("echo", "${MYTOOL_ADMIN_PORT}"))
        .assertExact(String.valueOf(env.ports().at(3)));
  }

  @Test
  @EnabledOnShell(BASH) // Single quotes are bash's; cmd.exe treats them as ordinary characters.
  public void aHarnessResolvedVariableIsNotLeftForTheShell() {
    // The proof that it is the harness and not bash doing the work: the resolved value reaches the
    // command even inside single quotes, where bash would never have expanded anything.
    session.allowShellSpecific();
    session.run("echo '${MYTOOL_ADMIN_PORT}'").assertExact(String.valueOf(env.ports().at(3)));
  }

  @Test
  public void unknownVariableFailsRatherThanExpandingToNothing() {
    for (String spelling : List.of("${NO_SUCH_VAR}", "$NO_SUCH_VAR", "{{NO_SUCH_VAR}}")) {
      AssertionError failure =
          assertThrows(AssertionError.class, () -> session.run("echo " + spelling));
      assertTrue(failure.getMessage(), failure.getMessage().contains("Unknown variable"));
      assertTrue(failure.getMessage(), failure.getMessage().contains("NO_SUCH_VAR"));
    }
  }

  @Test
  public void cmdOnlyVariableSpellingIsRejected() {
    // %NAME% cannot be substituted (% is String.format's), and leaving it alone would make a line
    // that works on Windows and fails on bash. So it is refused, pointing at ${NAME}.
    AssertionError failure =
        assertThrows(AssertionError.class, () -> session.run("echo %MYTOOL2_PORT%"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("cmd.exe-only variable"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("${MYTOOL2_PORT}"));
  }

  @Test
  public void aJavaValueIsConcatenated() {
    // The recommended way to get a Java-side value into a command: no format placeholder, no
    // binding, and it reads as the command it is.
    session.run("echo prefix-" + env.ports().at(3)).assertExact("prefix-" + env.ports().at(3));
  }

  @Test
  public void aConcatenatedValueIsStillScannedForVariables() {
    // The one sharp edge of concatenation: it happens before the harness resolves the line, so a
    // Java value that itself contains ${...} is read as a variable reference rather than text.
    String awkward = "${NO_SUCH_VAR}";
    AssertionError failure =
        assertThrows(AssertionError.class, () -> session.run("echo " + awkward));
    assertTrue(failure.getMessage(), failure.getMessage().contains("Unknown variable"));
  }

  @Test
  @EnabledOnShell(BASH) // About bash's brace expansion specifically.
  public void bashLeavesDoubleBracesAlone() {
    // Guards the choice of delimiter. 'not-a-var' is not a legal placeholder name, so the harness
    // passes it through untouched and bash gets to see it — unquoted, with brace expansion live.
    // Bash only expands braces containing a comma or '..', so it arrives verbatim.
    session.run("echo {{not-a-var}}").assertExact("{{not-a-var}}");
  }

  @Test
  @EnabledOnShell(BASH) // 'pwd' is the shell's own; the harness offers cwd() instead.
  public void cdChangesTheWorkingDirectory() throws IOException {
    // Compared as strings: the temporary directory may be reached through a symlink, while the
    // session resolves against its own cwd and produces a real path.
    Path target = Files.createDirectory(tmp.resolve("elsewhere")).toRealPath();
    session.cd(target);
    assertEquals(target.toString(), session.cwd().toString());
    session.allowShellSpecific();
    session.run("pwd").assertExitCode(0).assertExact(target.toString());
  }

  @Test
  public void cdRejectsAPathThatIsNotADirectory() {
    assertThrows(AssertionError.class, () -> session.cd("no-such-directory"));
  }

  @Test
  @EnabledOnShell(BASH) // 'exit' is spelled 'exit /b' on cmd.exe.
  public void exitStatusIsReported() {
    session.allowShellSpecific();
    session.tryRun("exit 3").assertExitCode(3);
    session.tryRun("exit 0").assertExitCode(0);
  }

  @Test
  @EnabledOnShell(BASH) // Writing to stderr at all needs the shell's own redirection syntax.
  public void stderrIsMergedByDefault() {
    session.allowShellSpecific();
    session.run("echo out; echo err 1>&2").assertContains("out").assertContains("err");
  }

  @Test
  public void stdoutAndStderrAreSeparableOnRequest() {
    var separated = session.runSeparated("echo ${MYTOOL_ADMIN_PORT}");
    assertEquals(String.valueOf(env.ports().at(3)), separated.stdout());
    assertEquals("", separated.stderr());
  }

  // cmd.exe's echo prints the quotes that quoting adds, so the expected output differs.
  @Test
  @EnabledOnShell(BASH)
  public void argvFormQuotesArgumentsForTheShell() {
    session.run(List.of("echo", "a b", "c&d", "e|f", "g'h")).assertExact("a b c&d e|f g'h");
  }

  @Test
  public void textBlockCommandsWorkWhateverTheClosingDelimiterDoes() {
    // A text block closed on its own line carries a trailing newline; the harness strips it, so
    // both spellings produce the same command.
    session
        .run(
            """
        echo hello
        """)
        .assertExact("hello");
    assertEquals("echo hello", session.lastResult().command());

    session
        .run(
            """
        echo hello""")
        .assertExact("hello");
    assertEquals("echo hello", session.lastResult().command());
  }

  @Test
  @EnabledOnShell(BASH) // 'sleep' is the shell's own; the harness offers awaitOrFail().
  public void aCommandThatNeverEndsFailsOnItsTimeout() {
    session.timeout(Duration.ofSeconds(2));
    session.allowShellSpecific();
    AssertionError failure = assertThrows(AssertionError.class, () -> session.run("sleep 60"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("did not finish within 2s"));
    assertTrue(session.lastResult().timedOut());
  }

  @Test
  @EnabledOnShell(BASH) // Prompting for input needs the shell's own 'read'.
  public void stdinIsClosedSoAPromptCannotHang() {
    // Redirecting stdin from the null device is what stops an interactive command blocking the
    // build: the prompt sees EOF at once. What matters is that the command returns at all, not
    // what the unread variable then expands to.
    session.timeout(Duration.ofSeconds(10));
    session.allowShellSpecific();
    session.run("read answer; echo done").assertContains("done");
    assertFalse(session.lastResult().timedOut());
  }

  // ------------------------------------------------------- standard input
  // Checked with 'cat', which does nothing but hand back what it was fed. That is bash's own, so
  // these are bash-only; the cmd.exe half of the primitive is covered by PostTest.stdinMode, whose
  // one line is the same on both shells.

  @Test
  @EnabledOnShell(BASH)
  public void stdinCarriesTextThroughUntouched() {
    session.allowShellSpecific();
    session.runWithStdin("plain text\n", "cat").assertExact("plain text");
  }

  @Test
  @EnabledOnShell(BASH)
  public void stdinCarriesSeveralLines() {
    session.allowShellSpecific();
    session
        .runWithStdin("first\nsecond\n", "cat")
        .assertLineCount(2)
        .assertContains("first")
        .assertContains("second");
  }

  @Test
  @EnabledOnShell(BASH)
  public void stdinCarriesQuotesNoShellWouldPassAsAnArgument() {
    session.allowShellSpecific();
    // Both quote characters at once, which cmd.exe cannot carry as an argument at all, plus the %
    // it would expand. This is the shape a test feeds a program that reads a document on stdin.
    String payload = "{'commit': {}, \"also\": \"100%\"}";
    session.runWithStdin(payload, "cat").assertExact(payload);
  }

  @Test
  @EnabledOnShell(BASH)
  public void stdinCarriesNonAsciiAsUtf8() {
    session.allowShellSpecific();
    session.runWithStdin("blåbær ☃\n", "cat").assertExact("blåbær ☃");
  }

  @Test
  @EnabledOnShell(BASH)
  public void stdinIsRecordedInTheTranscript() {
    session.allowShellSpecific();
    session.runWithStdin("recorded\n", "cat");
    String transcript = session.transcript().text();
    assertTrue(transcript, transcript.contains("on standard input:"));
    assertTrue(transcript, transcript.contains("recorded"));
  }

  @Test
  public void theTranscriptReadsLikeAShellSession() {
    session.run("echo first");
    session.run("echo second");
    String transcript = session.transcript().text();
    assertTrue(transcript, transcript.contains("$ echo first"));
    assertTrue(transcript, transcript.contains("| first"));
    assertTrue(transcript, transcript.contains("$ echo second"));
    assertTrue(transcript, transcript.contains("exit 0 in "));
  }

  @Test
  public void aFailureReportsEveryCommandAndCarriesNoStackTrace() {
    session.run("echo earlier-command");
    AssertionError failure =
        assertThrows(
            AssertionError.class, () -> session.run("echo haystack").assertContains("needle"));
    String message = failure.getMessage();

    // The whole history, so a reader sees how the test got here, not just the command that failed.
    assertTrue(message, message.contains("$ echo earlier-command"));
    assertTrue(message, message.contains("| earlier-command"));
    assertTrue(message, message.contains("$ echo haystack"));
    assertTrue(message, message.contains("| haystack"));

    // Then one line saying what was expected, and nothing else.
    assertTrue(message, message.contains("FAILED: output did not contain: needle"));

    // No Java frames: they would all be harness internals, and would bury the above.
    assertEquals(0, failure.getStackTrace().length);
  }

  @Test
  @EnabledOnShell(BASH) // 'exit' is spelled 'exit /b' on cmd.exe.
  public void aFailingExitStatusIsReportedPlainly() {
    session.allowShellSpecific();
    AssertionError failure =
        assertThrows(AssertionError.class, () -> session.tryRun("exit 3").assertExitCode(0));
    assertTrue(
        failure.getMessage(),
        failure.getMessage().contains("FAILED: command exited 3, expected 0"));
  }

  @Test
  @EnabledOnShell(BASH) // printf is the portable way to emit a specific invalid byte.
  public void outputThatIsNotValidUtf8DoesNotFailTheCommand() {
    // Found on Windows, where a launcher's .cmd output comes back in the console code page: strict
    // decoding turned a passing command into a MalformedInputException. Nothing guarantees a
    // command's bytes are UTF-8, so decoding replaces rather than throws.
    session.allowShellSpecific();
    session.run("printf 'before\\xffafter\\n'").assertExitCode(0).assertContains("before");
    session.lastResult().assertContains("after");
  }

  // ------------------------------------------------------- the program under test, on PATH

  @Test
  public void theProgramIsReachedByItsBareNameOnEitherShell() {
    // One spelling for both platforms: the launcher directory is on PATH, and PATHEXT is what
    // makes the bare word find mytool.cmd on Windows. The harness rewrites nothing.
    session.run("mytool echo hello").assertExitCode(0).assertExact("hello");
  }

  @Test
  public void anArgumentWithASpaceReachesTheProgramIntact() {
    session.run(List.of("mytool", "echo", "two words")).assertExitCode(0).assertExact("two words");
  }

  // ------------------------------------------------------- timing

  @Test
  public void durationAssertionsMeasureHowLongTheCommandTook() {
    CommandResult result = session.run("mytool work 1 0");
    result.assertDurationAtLeast(Duration.ofSeconds(1)).assertDurationAtMost(Duration.ofMinutes(5));

    AssertionError tooSlow =
        assertThrows(AssertionError.class, () -> result.assertDurationAtMost(Duration.ZERO));
    assertTrue(tooSlow.getMessage(), tooSlow.getMessage().contains("expected at most 0s"));

    AssertionError tooFast =
        assertThrows(
            AssertionError.class, () -> result.assertDurationAtLeast(Duration.ofMinutes(10)));
    assertTrue(tooFast.getMessage(), tooFast.getMessage().contains("expected at least 600s"));
  }

  // ------------------------------------------------------- background commands
  // The stand-in program prints a line, stays alive, prints another and exits with a given status,
  // which is every shape these cases need.

  @Test
  public void aBackgroundCommandIsReadableWhileItRuns() {
    BackgroundCommand command = session.runInBackground("mytool work 60 0");
    try {
      command.awaitOutput("starting", Duration.ofSeconds(60));
      assertFalse("it has not printed its last line yet", command.hasEnded());
      assertFalse(command.output(), command.output().contains("finished"));
      assertTrue(alive(command.pid()));

      String transcript = session.transcript().text();
      assertTrue(transcript, transcript.contains("$ mytool work 60 0 &"));
      assertTrue(transcript, transcript.contains("started as pid " + command.pid()));
    } finally {
      command.kill();
    }
  }

  @Test
  public void awaitOutputFailsWithWhatTheCommandHasPrintedSoFar() {
    BackgroundCommand command = session.runInBackground("mytool work 60 0");
    try {
      AssertionError failure =
          assertThrows(
              AssertionError.class,
              () -> command.awaitOutput("never printed", Duration.ofSeconds(2)));
      assertTrue(failure.getMessage(), failure.getMessage().contains("timed out after 2s"));
      assertTrue(failure.getMessage(), failure.getMessage().contains("never printed"));
      assertTrue("the output so far is the diagnosis", failure.getMessage().contains("starting"));
    } finally {
      command.kill();
    }
  }

  @Test
  public void waitForReturnsTheResultTheCommandExitedWith() {
    BackgroundCommand command = session.runInBackground("mytool work 1 3");
    CommandResult result = command.waitFor(Duration.ofMinutes(1));
    result
        .assertExitCode(3)
        .assertContains("starting")
        .assertContains("finished")
        .assertDurationAtLeast(Duration.ofSeconds(1));
    assertTrue(command.hasEnded());

    // The outcome is appended to the transcript when it ends, under the line that started it.
    String transcript = session.transcript().text();
    assertTrue(transcript, transcript.contains("$ mytool work 1 3 &"));
    assertTrue(transcript, transcript.contains("exit 3 in "));
    assertTrue(transcript, transcript.contains("| finished"));
  }

  @Test
  public void waitForKillsAndFailsWhenTheCommandOverrunsIt() {
    BackgroundCommand command = session.runInBackground("mytool work 60 0");
    AssertionError failure =
        assertThrows(AssertionError.class, () -> command.waitFor(Duration.ofSeconds(2)));
    assertTrue(failure.getMessage(), failure.getMessage().contains("did not finish within 2s"));
    assertTrue(command.hasEnded());
    assertFalse(alive(command.pid()));
  }

  @Test
  public void killStopsTheCommandAndIsIdempotent() {
    BackgroundCommand command = session.runInBackground("mytool work 60 0");
    command.awaitOutput("starting", Duration.ofSeconds(60));
    command.kill();
    assertTrue(command.hasEnded());
    assertFalse(alive(command.pid()));
    command.kill();
  }

  @Test
  public void aBackgroundCommandLeftRunningIsStoppedWhenTheSessionEnds() {
    CliSession child = session.child("forgot-to-wait");
    BackgroundCommand command = child.runInBackground("mytool work 60 0");
    command.awaitOutput("starting", Duration.ofSeconds(60));

    child.close();

    assertTrue("the teardown ended it", command.hasEnded());
    assertFalse(alive(command.pid()));
  }

  private static boolean alive(long pid) {
    return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
  }

  // ------------------------------------------------------- process ownership
  // Pids no process could have, so cleanup finds nothing alive and kills nothing.

  @Test
  public void aProcessIsOwnedByTheSessionThatStartedIt() {
    int port = env.ports().at(1);
    CliSession child = session.child("starter");
    try {
      program.running = List.of(RunningProcess.of(90_000_001L, port));
      child.run("echo started");
      assertEquals(Set.of(port), child.ownedPorts());
      assertEquals(Set.of(), session.ownedPorts());
    } finally {
      child.close();
    }
  }

  @Test
  public void aRestartedProcessStaysOwnedByTheAncestorSession() {
    // Otherwise a test that restarts the process its class started would take ownership of it, and
    // stop it when that one test ended — tearing down what every later test still expects.
    int port = env.ports().at(0);
    program.running = List.of(RunningProcess.of(90_000_002L, port));
    session.run("echo started");
    assertEquals(Set.of(port), session.ownedPorts());

    CliSession child = session.child("restarter");
    try {
      program.running = List.of(RunningProcess.of(90_000_003L, port));
      child.run("echo restarted");
      assertEquals(Set.of(port), session.ownedPorts());
      assertEquals(List.of(RunningProcess.of(90_000_003L, port)), session.ownedProcesses());
      assertEquals(List.of(), child.ownedProcesses());
    } finally {
      child.close();
    }
  }

  @Test
  public void aProcessOutsideThisClassesPortBlockIsNobodysBusiness() {
    // The record of running processes is shared with every other class on the cmd side, so the
    // block is what tells this class's processes from a sibling's.
    program.running = List.of(RunningProcess.of(90_000_004L, env.ports().at(0) + 500));
    session.run("echo elsewhere");
    assertEquals(List.of(), session.ownedProcesses());
  }

  @Test
  public void aProcessWithNoPortsIsOwnedOnItsMarkerAlone() {
    // Nothing to filter on: a program the harness finds by marker and that listens on nothing is
    // identified by its pid, and belongs to the session whose command made it appear.
    RunningProcess discovered = RunningProcess.of(90_000_005L);
    program.running = List.of(discovered);
    session.run("echo started");
    assertEquals(List.of(discovered), session.ownedProcesses());
    assertEquals(Set.of(), session.ownedPorts());
  }

  // ---------------------------------------------------------------- cmd.exe specifics

  @Test
  @EnabledOnShell(CMD)
  public void cmdQuotingProtectsMetacharacters() {
    // Characters cmd's parser would otherwise act on, passed as argv so the harness quotes each.
    // Checked with contains rather than equals because cmd's echo prints the quotes that quoting
    // has to add. Not via a batch variable: cmd expands %VAR% and then re-parses the result, so
    // "echo %ARG%" with ARG=a&b runs 'b' as a second command — the trap the harness avoids by
    // resolving ${NAME} itself.
    for (String nasty : List.of("a&b", "a|b", "a>b", "a<b", "a b")) {
      session.run(List.of("echo", nasty)).assertContains(nasty);
    }
  }

  @Test
  @EnabledOnShell(CMD)
  public void cmdQuotingRefusesACaret() {
    // 'call' re-parses its line, which doubles a caret: "a^b" arrives as "a^^b". Refused rather
    // than silently mangled, the same treatment an embedded double quote gets.
    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> session.run(List.of("echo", "a^b")));
    assertTrue(e.getMessage(), e.getMessage().contains("standard input"));
  }

  @Test
  @EnabledOnShell(CMD)
  public void cmdQuotingEscapesPercentForBatchFiles() {
    // % is expanded even inside double quotes, and a literal one in a batch file is written %%.
    // A URL carrying an escape like %20 is the case that matters.
    assertEquals("a%%20b", Shells.CMD.quote("a%20b"));
  }

  @Test
  @EnabledOnShell(CMD)
  public void cmdQuotingRefusesAnEmbeddedDoubleQuote() {
    // Refused rather than mangled: cmd has no escape that survives both its own quote counting
    // and the target program's parser.
    UnsupportedOperationException e =
        assertThrows(
            UnsupportedOperationException.class, () -> session.run(List.of("echo", "a\"b")));
    assertTrue(e.getMessage(), e.getMessage().contains("standard input"));
  }

  @Test
  @EnabledOnShell(CMD)
  public void cmdEnvironmentIsCaseInsensitive() {
    // Windows environment names are case-insensitive, so the harness keeps its env map that way.
    session.env("MiXeD_Case_Var", "value");
    assertEquals("value", session.env("mixed_case_var"));
  }

  // ------------------------------------------------------- portability rails

  @Test
  public void aPipeIsRejectedWithTheAlternative() {
    AssertionError failure =
        assertThrows(AssertionError.class, () -> session.run("echo hello | grep hello"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("a pipe (|) is not portable"));
  }

  @Test
  public void aUnixOnlyToolIsRejectedWithTheAlternative() {
    AssertionError failure =
        assertThrows(AssertionError.class, () -> session.run("grep -c hello somefile.txt"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("linesMatching()"));
    // And the same mistake spelled with a path.
    assertThrows(AssertionError.class, () -> session.run("/usr/bin/awk {print} f"));
    // Also in the argv form, where only the tool name can be judged.
    assertThrows(AssertionError.class, () -> session.run(List.of("sed", "s/a/b/", "f")));
  }

  @Test
  public void chainedCommandsAndSubstitutionsAreRejected() {
    assertThrows(AssertionError.class, () -> session.run("echo a && echo b"));
    assertThrows(AssertionError.class, () -> session.run("echo a ; echo b"));
    assertThrows(AssertionError.class, () -> session.run("echo $(date)"));
    assertThrows(AssertionError.class, () -> session.run("echo > /dev/null"));
    AssertionError assignment =
        assertThrows(AssertionError.class, () -> session.run("LOG_LEVEL=DEBUG echo hi"));
    assertTrue(assignment.getMessage(), assignment.getMessage().contains("env(\"LOG_LEVEL\""));
  }

  @Test
  public void singleQuotesAreRejectedBecauseCmdHasNoSuchQuoting() {
    AssertionError failure =
        assertThrows(AssertionError.class, () -> session.run("echo 'hello world'"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("single-quote quoting"));
  }

  @Test
  public void aMetacharacterInsideDoubleQuotesIsFine() {
    // Both shells protect it, so a URL with a query string must not trip the rails.
    session
        .run("echo \"http://localhost:${MYTOOL_PORT}/items?q=name:a*&rows=10\"")
        .assertContains("&rows=10");
  }

  // ------------------------------------------------------- output extraction

  @Test
  @EnabledOnShell(BASH) // Emitting three fixed lines needs the shell's own 'printf'.
  public void outputExtractionReplacesTheUnixTextTools() {
    session.allowShellSpecific();
    var result = session.run("printf 'alpha 1\\nbeta 2\\ngamma 3\\n'");
    result.assertLineCount(3);
    assertEquals(List.of("beta 2"), result.linesMatching("^beta"));
    assertEquals(List.of("alpha 1"), result.firstLines(1));
    assertEquals(List.of("gamma 3"), result.lastLines(1));
    assertEquals("2", result.fields(1).get(1));
    assertEquals("beta", result.firstMatch("(beta)"));
    assertEquals(5, result.countOccurrences("a"));
    result.assertContainsTimes("beta", 1);
  }

  // ------------------------------------------------------- file work

  @Test
  public void fileHelpersReplaceTheShellFileTools() throws IOException {
    session.write("dir/file.txt", "one\ntwo\n");
    assertTrue(session.exists("dir/file.txt"));
    assertEquals("one\ntwo\n", session.read("dir/file.txt"));
    session.append("dir/file.txt", "three\n");
    assertTrue(session.fileContains("dir/file.txt", "three"));
    assertEquals(List.of("two"), session.readLinesMatching("dir/file.txt", "^two$"));

    session.mkdirs("dir/nested");
    session.copy("dir/file.txt", "dir/nested/copy.txt");
    assertEquals(
        List.of("copy.txt"),
        session.list("dir/nested").stream().map(f -> f.getFileName().toString()).toList());
    assertEquals(1, session.list("dir", "*.txt").size());
    assertEquals(1, session.list("dir", "**/*.txt").size());

    session.copy("dir", "tree");
    assertTrue(session.exists("tree/nested/copy.txt"));

    session.rm("dir");
    assertFalse(session.exists("dir"));
    // Removing what is not there is not an error, as 'rm -rf' is not.
    session.rm("dir");
  }

  @Test
  public void relativePathsResolveAgainstTheSessionDirectory() {
    assertEquals(session.cwd().resolve("x").toString(), session.path("x").toString());
  }

  @Test
  public void headerAndRegexpAssertionsCoverTheCurlDashICases() {
    CatsCliHelpers.HttpResult result =
        new CatsCliHelpers.HttpResult(
            200,
            "<html><script src=\"libs/app/index.js\"></script></html>",
            HttpHeaders.of(
                Map.of("Content-Type", List.of("text/html;charset=utf-8")), (name, value) -> true),
            Map.of("MYTOOL_PORT", "1234"),
            env.echoedPrefixes());
    // ${NAME} resolves in an HTTP assertion exactly as it does in a command assertion; before
    // this, the expected text was compared unresolved and the assertion passed silently.
    result.assertContains("libs/app/index.js").assertNotContains("port ${MYTOOL_PORT}");
    result
        .assertHeaderContains("Content-Type: text/html")
        .assertHeaderNotContains("Cache-Control")
        .assertMatches("<html")
        // The versioned-asset shape a web UI must not emit: a digest before the extension.
        .assertNotMatches("(src|href)=\"[^\"]+\\.[0-9a-f]{8}\\.(js|css)\"");
    assertThrows(AssertionError.class, () -> result.assertHeaderContains("X-Frame-Options"));
    assertThrows(
        AssertionError.class, () -> result.assertNotMatches("(src|href)=\"[^\"]+\\.(js|css)\""));
  }

  @Test
  public void anHttpRequestCarryingHeadersReportsAConnectionFailureTheSameWay() {
    // The overload that ports 'curl -H "Name: value"'; nothing is listening on this session's
    // block, so it exercises the request path without needing a server.
    session
        .helpers()
        .http(session.ports().at(0), "/items/select", Map.of("Accept-Encoding", "gzip"))
        .assertConnectionFailed();
  }

  @Test
  public void aMultipartFilePostReportsAConnectionFailureTheSameWay() {
    // The overload that ports 'curl -F "field=@file"'; nothing is listening on this session's
    // block, so it exercises building and sending the multipart body without needing a server.
    Path document = session.write("doc.bin", "a document");
    session
        .helpers()
        .httpPostFile(session.ports().at(0), "/upload?id=doc1", "myfile", document)
        .assertConnectionFailed();
  }

  @Test
  public void anHttpsRequestThatCannotConnectIsReportedAsAFailureNotAStatus() {
    // Nothing is listening on the block this session claimed, which is the shape of a test that
    // expects a request to fail: no status, and the reason readable in the body.
    CatsCliHelpers.HttpResult result =
        session.helpers().https(session.ports().at(0), "/status").get();
    result.assertConnectionFailed();
    assertTrue(result.body(), result.body().contains("Connect"));
  }

  @Test
  public void aPostOrPutThatCannotConnectIsReportedAsAFailureNotAStatus() {
    // The body-carrying ports of 'curl -X POST -d ...' and 'curl --data-binary @file -X PUT ...'.
    // Nothing is listening on the block this session claimed, so this exercises the request path
    // without needing a server.
    int port = session.ports().at(0);
    session
        .helpers()
        .httpPost(port, "/items", "application/json", "{\"name\":\"one\"}")
        .assertConnectionFailed();
    Path file = session.write("upload.txt", "payload");
    session
        .helpers()
        .httpPut(port, "/files/upload.txt", "application/octet-stream", file)
        .assertConnectionFailed();
  }

  @Test
  public void aDownloadThatCannotConnectFailsLoudlyRatherThanLeavingATruncatedFile() {
    // Port of 'curl -o <file> <url>'. Nothing is listening on this session's block.
    String url = "http://localhost:" + session.ports().at(0) + "/model.onnx";
    assertThrows(UncheckedIOException.class, () -> session.download(url, "model.onnx"));
  }

  // ------------------------------------------------------- a local web server

  @Test
  public void serveHttpServesTheDirectoryAndStopsOnRequest() {
    session.write("site/index.html", "<html><body>hello from the fixture</body></html>");
    session.write("site/data.json", "{\"k\":1}");
    int port = session.serveHttp("site");
    assertTrue("an ephemeral port was chosen", port > 0);

    // The directory's index.html is what / serves, and the content type follows the extension.
    session
        .helpers()
        .http(port, "/")
        .assertStatus(200)
        .assertContains("hello from the fixture")
        .assertHeaderContains("content-type: text/html");
    session.helpers().http(port, "/data.json").assertStatus(200).assertContains("\"k\":1");
    session.helpers().http(port, "/nope.html").assertStatus(404);

    // Stopping it early is what a test does to watch a fetch fail; stopping twice is harmless.
    session.serveHttpShutdown(port);
    session.serveHttpShutdown(port);
    session.helpers().http(port, "/").assertConnectionFailed();
  }

  @Test
  public void serveHttpRefusesSomethingThatIsNotADirectory() {
    session.write("afile.txt", "x");
    AssertionError failure =
        assertThrows(AssertionError.class, () -> session.serveHttp("afile.txt"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("not a directory"));
  }

  @Test
  public void closingTheSessionStopsAServerTheTestForgot() {
    session.write("site/index.html", "<html><body>x</body></html>");
    int port = session.serveHttp("site");
    session.helpers().http(port, "/").assertStatus(200);
    session.close();
    session.helpers().http(port, "/").assertConnectionFailed();
  }
}
