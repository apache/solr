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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.shell.env.HarnessEnv;
import org.apache.solr.shell.exec.Shell;
import org.apache.solr.shell.session.CliSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for the harness itself — {@code env}, {@code cd}, {@code run}, {@code script},
 * variable resolution, quoting, timeouts — with no Solr involved.
 *
 * <p>Deliberately uses only shell built-ins, so it needs no unpacked distribution and runs in a
 * couple of seconds. It runs on whichever shell the platform uses, which is how the {@code
 * Shell.CMD} half of the harness gets covered at all; the handful of cases that really are one
 * shell's carry {@link EnabledOnShell}. The suites that actually drive {@code bin/solr} live in
 * {@code solr/packaging} and extend {@link SolrShellTestCase}.
 */
public class SolrShellSessionTest extends SolrTestCase {

  /** Makes {@link EnabledOnShell} effective without needing a Solr distribution. */
  @Rule public final ShellGate shellGate = new ShellGate();

  private HarnessEnv env;
  private CliSession session;

  @Before
  public void openSession() throws IOException {
    // HarnessEnv only asks that SOLR_TIP contain a bin directory; nothing here runs bin/solr.
    Path fakeTip = createTempDir("tip");
    Files.createDirectories(fakeTip.resolve("bin"));
    env = HarnessEnv.of(fakeTip, createTempDir("out"));
    session = CliSession.forScope(env, getTestName());
  }

  @After
  public void closeSession() {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void envReachesTheShell() {
    session.env("MARKER", "hello world");
    session
        .script("echo \"$MARKER\"", "echo %MARKER%")
        .assertSuccess()
        .assertOutputEquals("hello world");
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
    session
        .script("echo \"$SOLR_HOME\"", "echo %SOLR_HOME%")
        .assertSuccess()
        .assertOutputEquals(env.solrHome().toString());
  }

  @Test
  public void runResolvesEveryVariableSpelling() {
    // All three mean "the environment variable", resolved by the harness before any shell sees it.
    // ${NAME} is the one BATS uses, so a ported line needs no edit.
    String zk = String.valueOf(env.ports().zk());
    session.run("echo ${ZK_PORT}").assertSuccess().assertOutputEquals(zk);
    session.run("echo $ZK_PORT").assertSuccess().assertOutputEquals(zk);
    session.run("echo {{ZK_PORT}}").assertSuccess().assertOutputEquals(zk);
  }

  @Test
  public void assertionsResolveVariablesToo() {
    // The same spelling means the same thing in an assertion as in a command line, so a BATS
    // assert_output --partial "...${ZK_PORT}" ports over unchanged.
    session.run("echo port-${ZK_PORT}").assertOutputContains("port-${ZK_PORT}");
    session.run("echo port-${ZK_PORT}").assertOutputEquals("port-${ZK_PORT}");
    session.run("echo nothing").assertOutputNotContains("port-${ZK_PORT}");
  }

  @Test
  public void anUnknownVariableInAnAssertionFailsToo() {
    AssertionError failure =
        expectThrows(
            AssertionError.class,
            () -> session.run("echo hi").assertOutputContains("${NO_SUCH_VAR}"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("Unknown variable"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("an assertion"));
  }

  @Test
  public void runResolvesVariablesTheTestSetItself() {
    session.env("MARKER", "from-the-test");
    session.run("echo ${MARKER}").assertSuccess().assertOutputEquals("from-the-test");
  }

  @Test
  public void runResolvesVariablesInTheArgvForm() {
    session
        .run(List.of("echo", "${ZK_PORT}"))
        .assertSuccess()
        .assertOutputEquals(String.valueOf(env.ports().zk()));
  }

  @Test
  @EnabledOnShell(
      Shell.BASH) // Single quotes are bash's; cmd.exe treats them as ordinary characters.
  public void aHarnessResolvedVariableIsNotLeftForTheShell() {
    // The proof that it is the harness and not bash doing the work: the resolved value reaches the
    // command even inside single quotes, where bash would never have expanded anything.
    session
        .run("echo '${ZK_PORT}'")
        .assertSuccess()
        .assertOutputEquals(String.valueOf(env.ports().zk()));
  }

  @Test
  public void unknownVariableFailsRatherThanExpandingToNothing() {
    for (String spelling : List.of("${NO_SUCH_VAR}", "$NO_SUCH_VAR", "{{NO_SUCH_VAR}}")) {
      AssertionError failure =
          expectThrows(AssertionError.class, () -> session.run("echo " + spelling));
      assertTrue(failure.getMessage(), failure.getMessage().contains("Unknown variable"));
      assertTrue(failure.getMessage(), failure.getMessage().contains("NO_SUCH_VAR"));
    }
  }

  @Test
  public void cmdOnlyVariableSpellingIsRejected() {
    // %NAME% cannot be substituted (% is String.format's), and leaving it alone would make a line
    // that works on Windows and fails on bash. So it is refused, pointing at ${NAME}.
    AssertionError failure =
        expectThrows(AssertionError.class, () -> session.run("echo %SOLR2_PORT%"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("cmd.exe-only variable"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("${SOLR2_PORT}"));
  }

  @Test
  public void formatArgsAndVariablesCoexist() {
    session
        .run("echo %s-${ZK_PORT}", "prefix")
        .assertSuccess()
        .assertOutputEquals("prefix-" + env.ports().zk());
  }

  @Test
  public void scriptLeavesShellVariablesToTheShell() {
    // A script has its own locals and positional parameters, so the harness must not touch $NAME.
    // Here bash expands ZK_PORT from the environment, and sets and reads a local of its own.
    session
        .script("mine=abc; echo \"$ZK_PORT-$mine\"", "set mine=abc\r\necho %ZK_PORT%-%mine%")
        .assertSuccess()
        .assertOutputEquals(env.ports().zk() + "-abc");
  }

  @Test
  public void scriptStillResolvesDoubleBraces() {
    session
        .script("echo {{ZK_PORT}}", "echo {{ZK_PORT}}")
        .assertSuccess()
        .assertOutputEquals(String.valueOf(env.ports().zk()));
  }

  @Test
  public void scriptSkipsWhenTheCurrentShellHasNoBody() {
    boolean bash = session.shell() == Shell.BASH;
    expectThrows(
        org.junit.AssumptionViolatedException.class,
        () -> {
          if (bash) {
            session.script(null, "echo cmd-only");
          } else {
            session.script("echo bash-only", null);
          }
        });
  }

  @Test
  @EnabledOnShell(Shell.BASH) // About bash's brace expansion specifically.
  public void bashLeavesDoubleBracesAlone() {
    // Guards the choice of delimiter. 'not-a-var' is not a legal placeholder name, so the harness
    // passes it through untouched and bash gets to see it — unquoted, with brace expansion live.
    // Bash only expands braces containing a comma or '..', so it arrives verbatim.
    session
        .script("echo {{not-a-var}}", "echo {{not-a-var}}")
        .assertSuccess()
        .assertOutputEquals("{{not-a-var}}");
  }

  @Test
  public void cdChangesTheWorkingDirectory() throws IOException {
    // Compared as strings: createTempDir hands back a Lucene FilterPath, while the session
    // resolves against its own cwd and produces a plain filesystem Path.
    Path target = createTempDir("elsewhere").toRealPath();
    session.cd(target);
    assertEquals(target.toString(), session.cwd().toString());
    session.script("pwd", "cd").assertSuccess().assertOutputEquals(target.toString());
  }

  @Test
  public void cdRejectsAPathThatIsNotADirectory() {
    expectThrows(AssertionError.class, () -> session.cd("no-such-directory"));
  }

  @Test
  public void exitStatusIsReported() {
    session.script("exit 3", "exit /b 3").assertFailure().assertStatus(3);
    session.script("exit 0", "exit /b 0").assertSuccess().assertStatus(0);
  }

  @Test
  public void stderrIsMergedByDefaultAndSeparableOnRequest() {
    session
        .script("echo out; echo err 1>&2", "echo out\r\necho err 1>&2")
        .assertSuccess()
        .assertOutputContains("err");

    var separated = session.runSeparated("echo ${ZK_PORT}");
    separated.assertSuccess();
    assertEquals(String.valueOf(env.ports().zk()), separated.stdout());
    assertEquals("", separated.stderr());
  }

  @Test
  @EnabledOnShell(
      Shell.BASH) // cmd.exe's echo prints the quotes that quoting adds, so the expected output
  // differs.
  public void argvFormQuotesArgumentsForTheShell() {
    session
        .run(List.of("echo", "a b", "c&d", "e|f", "g'h"))
        .assertSuccess()
        .assertOutputEquals("a b c&d e|f g'h");
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
        .assertOutputEquals("hello");
    assertEquals("echo hello", session.lastResult().command());

    session
        .run(
            """
        echo hello""")
        .assertOutputEquals("hello");
    assertEquals("echo hello", session.lastResult().command());
  }

  @Test
  public void aCommandThatNeverEndsFailsOnItsTimeout() {
    session.timeout(Duration.ofSeconds(2));
    AssertionError failure =
        expectThrows(
            AssertionError.class, () -> session.script("sleep 60", "ping -n 61 127.0.0.1 >nul"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("did not finish within 2s"));
    assertTrue(session.lastResult().timedOut());
  }

  @Test
  public void stdinIsClosedSoAPromptCannotHang() {
    // Redirecting stdin from the null device is what stops an interactive command blocking the
    // build: the prompt sees EOF at once. What matters cross-shell is that it returns at all, not
    // what an unset variable then expands to, which the two shells spell differently.
    session.timeout(Duration.ofSeconds(10));
    session
        .script("read answer; echo done", "set /p answer=\r\necho done")
        .assertOutputContains("done");
    assertFalse(session.lastResult().timedOut());
  }

  @Test
  public void theTranscriptRecordsEachCommand() {
    session.run("echo first");
    session.run("echo second");
    String transcript = session.transcript().text();
    assertTrue(transcript, transcript.contains("echo first"));
    assertTrue(transcript, transcript.contains("echo second"));
    assertTrue("the resolved status belongs in the transcript", transcript.contains("status  : 0"));
  }

  @Test
  public void assertionFailuresCarryTheWholeTranscriptEntry() {
    AssertionError failure =
        expectThrows(
            AssertionError.class,
            () -> session.run("echo haystack").assertOutputContains("needle"));
    String message = failure.getMessage();
    assertTrue(message, message.contains("expected output to contain: needle"));
    assertTrue(message, message.contains("command : echo haystack"));
    assertTrue(message, message.contains("status  : 0"));
    assertTrue(message, message.contains("haystack"));
  }

  @Test
  @EnabledOnShell(Shell.BASH) // printf is the portable way to emit a specific invalid byte.
  public void outputThatIsNotValidUtf8DoesNotFailTheCommand() {
    // Found on Windows, where bin/solr.cmd output comes back in the console code page: strict
    // decoding turned a passing command into a MalformedInputException. Nothing guarantees a
    // command's bytes are UTF-8, so decoding replaces rather than throws.
    session
        .script("printf 'before\\xffafter\\n'", null)
        .assertSuccess()
        .assertOutputContains("before");
    session.lastResult().assertOutputContains("after");
  }

  // ---------------------------------------------------------------- cmd.exe specifics

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdQuotingProtectsMetacharacters() {
    // Characters cmd's parser would otherwise act on: & separates commands, | pipes, > redirects.
    // Round-tripped through a batch variable so the quotes cmd adds are not part of the comparison.
    for (String nasty : List.of("a&b", "a|b", "a>b", "a<b", "a^b", "a b")) {
      session.env("ARG", nasty);
      session.script(null, "echo %ARG%").assertSuccess().assertOutputEquals(nasty);
    }
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdQuotingEscapesPercentForBatchFiles() {
    // % is expanded even inside double quotes, and a literal one in a batch file is written %%.
    // A URL carrying an escape like %20 is the case that matters.
    assertEquals("a%%20b", Shell.CMD.quote("a%20b"));
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdQuotingRefusesAnEmbeddedDoubleQuote() {
    // Refused rather than mangled: cmd has no escape that survives both its own quote counting
    // and the target program's parser.
    UnsupportedOperationException e =
        expectThrows(
            UnsupportedOperationException.class, () -> session.run(List.of("echo", "a\"b")));
    assertTrue(e.getMessage(), e.getMessage().contains("script(bashBody, cmdBody)"));
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdScriptsMayBeMultiLine() {
    // 'call' takes one command, so a multi-line body is written verbatim instead. This is what
    // script(bash, cmd) exists for.
    session
        .script(null, "set first=one\r\nset second=two\r\necho %first%-%second%")
        .assertSuccess()
        .assertOutputEquals("one-two");
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdEnvironmentIsCaseInsensitive() {
    // Windows environment names are case-insensitive, so the harness keeps its env map that way.
    session.env("MiXeD_Case_Var", "value");
    assertEquals("value", session.env("mixed_case_var"));
    session.script(null, "echo %MIXED_CASE_VAR%").assertSuccess().assertOutputEquals("value");
  }
}
