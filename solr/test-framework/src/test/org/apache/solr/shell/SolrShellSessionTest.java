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
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for the harness itself — {@code env}, {@code cd}, {@code run}, {@code script},
 * variable resolution, quoting, timeouts — with no Solr involved.
 *
 * <p>Deliberately uses only shell built-ins, so it needs no unpacked distribution and runs in a
 * couple of seconds. The suites that actually drive {@code bin/solr} live in {@code solr/packaging}
 * and extend {@link SolrShellTestCase}.
 */
public class SolrShellSessionTest extends SolrTestCase {

  private HarnessEnv env;
  private CliSession session;

  @BeforeClass
  public static void checkShell() {
    // Every command below is bash; the cmd.exe half of the harness is still a stub.
    assumeTrue("Harness unit tests are bash-only for now", Shell.defaultShell() == Shell.BASH);
  }

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
    session.script("echo \"$MARKER\"", null).assertSuccess().assertOutputEquals("hello world");
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
        .script("echo \"$SOLR_HOME\"", null)
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
        .script("mine=abc; echo \"$ZK_PORT-$mine\"", null)
        .assertSuccess()
        .assertOutputEquals(env.ports().zk() + "-abc");
  }

  @Test
  public void scriptStillResolvesDoubleBraces() {
    session
        .script("echo {{ZK_PORT}}", null)
        .assertSuccess()
        .assertOutputEquals(String.valueOf(env.ports().zk()));
  }

  @Test
  public void scriptSkipsWhenTheCurrentShellHasNoBody() {
    expectThrows(
        org.junit.AssumptionViolatedException.class, () -> session.script(null, "echo cmd-only"));
  }

  @Test
  public void bashLeavesDoubleBracesAlone() {
    // Guards the choice of delimiter. 'not-a-var' is not a legal placeholder name, so the harness
    // passes it through untouched and bash gets to see it — unquoted, with brace expansion live.
    // Bash only expands braces containing a comma or '..', so it arrives verbatim.
    session.script("echo {{not-a-var}}", null).assertSuccess().assertOutputEquals("{{not-a-var}}");
  }

  @Test
  public void cdChangesTheWorkingDirectory() throws IOException {
    // Compared as strings: createTempDir hands back a Lucene FilterPath, while the session
    // resolves against its own cwd and produces a plain filesystem Path.
    Path target = createTempDir("elsewhere").toRealPath();
    session.cd(target);
    assertEquals(target.toString(), session.cwd().toString());
    session.script("pwd", null).assertSuccess().assertOutputEquals(target.toString());
  }

  @Test
  public void cdRejectsAPathThatIsNotADirectory() {
    expectThrows(AssertionError.class, () -> session.cd("no-such-directory"));
  }

  @Test
  public void exitStatusIsReported() {
    session.script("exit 3", null).assertFailure().assertStatus(3);
    session.script("exit 0", null).assertSuccess().assertStatus(0);
  }

  @Test
  public void stderrIsMergedByDefaultAndSeparableOnRequest() {
    session.script("echo out; echo err 1>&2", null).assertSuccess().assertOutputContains("err");

    var separated = session.runSeparated("echo ${ZK_PORT}");
    separated.assertSuccess();
    assertEquals(String.valueOf(env.ports().zk()), separated.stdout());
    assertEquals("", separated.stderr());
  }

  @Test
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
        expectThrows(AssertionError.class, () -> session.script("sleep 60", null));
    assertTrue(failure.getMessage(), failure.getMessage().contains("did not finish within 2s"));
    assertTrue(session.lastResult().timedOut());
  }

  @Test
  public void stdinIsClosedSoAPromptCannotHang() {
    // Redirecting stdin from the null device is what stops an interactive command blocking the
    // build: 'read' sees EOF at once and returns non-zero rather than waiting.
    session.timeout(Duration.ofSeconds(10));
    session.script("read answer; echo \"got:$answer\"", null).assertOutputEquals("got:");
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
}
