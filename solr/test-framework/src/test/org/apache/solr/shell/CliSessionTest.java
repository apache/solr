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
import org.apache.solr.shell.session.SolrCliHelpers;
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
public class CliSessionTest extends SolrTestCase {

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
    session.script("echo \"$MARKER\"", "echo %MARKER%").assertSuccess().assertExact("hello world");
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
        .assertExact(env.solrHome().toString());
  }

  @Test
  public void runResolvesEveryVariableSpelling() {
    // All three mean "the environment variable", resolved by the harness before any shell sees it.
    // ${NAME} is the one BATS uses, so a ported line needs no edit.
    String zk = String.valueOf(env.ports().zk());
    session.run("echo ${ZK_PORT}").assertSuccess().assertExact(zk);
    session.run("echo $ZK_PORT").assertSuccess().assertExact(zk);
    session.run("echo {{ZK_PORT}}").assertSuccess().assertExact(zk);
  }

  @Test
  public void assertionsResolveVariablesToo() {
    // The same spelling means the same thing in an assertion as in a command line, so a BATS
    // assert_output --partial "...${ZK_PORT}" ports over unchanged.
    session.run("echo port-${ZK_PORT}").assertContains("port-${ZK_PORT}");
    session.run("echo port-${ZK_PORT}").assertExact("port-${ZK_PORT}");
    session.run("echo nothing").assertNotContains("port-${ZK_PORT}");
  }

  @Test
  public void anUnknownVariableInAnAssertionFailsToo() {
    AssertionError failure =
        expectThrows(
            AssertionError.class, () -> session.run("echo hi").assertContains("${NO_SUCH_VAR}"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("Unknown variable"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("an assertion"));
  }

  @Test
  public void runResolvesVariablesTheTestSetItself() {
    session.env("MARKER", "from-the-test");
    session.run("echo ${MARKER}").assertSuccess().assertExact("from-the-test");
  }

  @Test
  public void runResolvesVariablesInTheArgvForm() {
    session
        .run(List.of("echo", "${ZK_PORT}"))
        .assertSuccess()
        .assertExact(String.valueOf(env.ports().zk()));
  }

  @Test
  @EnabledOnShell(
      Shell.BASH) // Single quotes are bash's; cmd.exe treats them as ordinary characters.
  public void aHarnessResolvedVariableIsNotLeftForTheShell() {
    // The proof that it is the harness and not bash doing the work: the resolved value reaches the
    // command even inside single quotes, where bash would never have expanded anything.
    session
        .allowShellSpecific()
        .run("echo '${ZK_PORT}'")
        .assertSuccess()
        .assertExact(String.valueOf(env.ports().zk()));
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
  public void aJavaValueIsConcatenated() {
    // The recommended way to get a Java-side value into a command: no format placeholder, no
    // binding, and it reads as the command it is.
    session
        .run("echo prefix-" + env.ports().zk())
        .assertSuccess()
        .assertExact("prefix-" + env.ports().zk());
  }

  @Test
  public void aConcatenatedValueIsStillScannedForVariables() {
    // The one sharp edge of concatenation: it happens before the harness resolves the line, so a
    // Java value that itself contains ${...} is read as a variable reference rather than text.
    String awkward = "${NO_SUCH_VAR}";
    AssertionError failure =
        expectThrows(AssertionError.class, () -> session.run("echo " + awkward));
    assertTrue(failure.getMessage(), failure.getMessage().contains("Unknown variable"));
  }

  @Test
  public void scriptLeavesShellVariablesToTheShell() {
    // A script has its own locals and positional parameters, so the harness must not touch $NAME.
    // Here bash expands ZK_PORT from the environment, and sets and reads a local of its own.
    session
        .script("mine=abc; echo \"$ZK_PORT-$mine\"", "set mine=abc\r\necho %ZK_PORT%-%mine%")
        .assertSuccess()
        .assertExact(env.ports().zk() + "-abc");
  }

  @Test
  public void scriptStillResolvesDoubleBraces() {
    session
        .script("echo {{ZK_PORT}}", "echo {{ZK_PORT}}")
        .assertSuccess()
        .assertExact(String.valueOf(env.ports().zk()));
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
        .assertExact("{{not-a-var}}");
  }

  @Test
  public void cdChangesTheWorkingDirectory() throws IOException {
    // Compared as strings: createTempDir hands back a Lucene FilterPath, while the session
    // resolves against its own cwd and produces a plain filesystem Path.
    Path target = createTempDir("elsewhere").toRealPath();
    session.cd(target);
    assertEquals(target.toString(), session.cwd().toString());
    session.script("pwd", "cd").assertSuccess().assertExact(target.toString());
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
        .assertContains("err");

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
        .assertExact("a b c&d e|f g'h");
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
    session.script("read answer; echo done", "set /p answer=\r\necho done").assertContains("done");
    assertFalse(session.lastResult().timedOut());
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
        expectThrows(
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
  public void aFailingExitStatusIsReportedPlainly() {
    AssertionError failure =
        expectThrows(
            AssertionError.class, () -> session.script("exit 3", "exit /b 3").assertSuccess());
    assertTrue(
        failure.getMessage(),
        failure.getMessage().contains("FAILED: command exited 3, expected 0"));
  }

  @Test
  @EnabledOnShell(Shell.BASH) // printf is the portable way to emit a specific invalid byte.
  public void outputThatIsNotValidUtf8DoesNotFailTheCommand() {
    // Found on Windows, where bin/solr.cmd output comes back in the console code page: strict
    // decoding turned a passing command into a MalformedInputException. Nothing guarantees a
    // command's bytes are UTF-8, so decoding replaces rather than throws.
    session.script("printf 'before\\xffafter\\n'", null).assertSuccess().assertContains("before");
    session.lastResult().assertContains("after");
  }

  // ---------------------------------------------------------------- cmd.exe specifics

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdQuotingProtectsMetacharacters() {
    // Characters cmd's parser would otherwise act on: & separates commands, | pipes, > redirects.
    // Passed as argv so the harness quotes each one.
    //
    // Checked with contains rather than equals because cmd's echo prints the quotes that quoting
    // has to add. Note also why this does not go through a batch variable: cmd expands %VAR% and
    // then re-parses the result, so "echo %ARG%" with ARG=a&b runs 'b' as a second command. That
    // re-parse is exactly the trap the harness avoids by resolving ${NAME} itself.
    for (String nasty : List.of("a&b", "a|b", "a>b", "a<b", "a b")) {
      session.run(List.of("echo", nasty)).assertSuccess().assertContains(nasty);
    }
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdQuotingRefusesACaret() {
    // 'call' re-parses its line, which doubles a caret: "a^b" arrives as "a^^b". Refused rather
    // than silently mangled, the same treatment an embedded double quote gets.
    UnsupportedOperationException e =
        expectThrows(
            UnsupportedOperationException.class, () -> session.run(List.of("echo", "a^b")));
    assertTrue(e.getMessage(), e.getMessage().contains("script(bashBody, cmdBody)"));
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
        .assertExact("one-two");
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void aCmdScriptRejectsADollarVariable() {
    // cmd.exe does not expand $NAME, so it would reach the command as text. Caught because the
    // bash body of the same script() call does expand it, which makes the mistake look fine until
    // it runs on Windows.
    AssertionError failure =
        expectThrows(
            AssertionError.class, () -> session.script("echo ${ZK_PORT}", "echo ${ZK_PORT}"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("{{ZK_PORT}}"));
  }

  @Test
  @EnabledOnShell(Shell.CMD)
  public void cmdEnvironmentIsCaseInsensitive() {
    // Windows environment names are case-insensitive, so the harness keeps its env map that way.
    session.env("MiXeD_Case_Var", "value");
    assertEquals("value", session.env("mixed_case_var"));
    session.script(null, "echo %MIXED_CASE_VAR%").assertSuccess().assertExact("value");
  }

  // ------------------------------------------------------- portability rails

  @Test
  public void aPipeIsRejectedWithTheAlternative() {
    AssertionError failure =
        expectThrows(AssertionError.class, () -> session.run("echo hello | grep hello"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("a pipe (|) is not portable"));
  }

  @Test
  public void aUnixOnlyToolIsRejectedWithTheAlternative() {
    AssertionError failure =
        expectThrows(AssertionError.class, () -> session.run("grep -c solr somefile.txt"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("linesMatching()"));
    // And the same mistake spelled with a path.
    expectThrows(AssertionError.class, () -> session.run("/usr/bin/awk {print} f"));
    // Also in the argv form, where only the tool name can be judged.
    expectThrows(AssertionError.class, () -> session.run(List.of("sed", "s/a/b/", "f")));
  }

  @Test
  public void chainedCommandsAndSubstitutionsAreRejected() {
    expectThrows(AssertionError.class, () -> session.run("echo a && echo b"));
    expectThrows(AssertionError.class, () -> session.run("echo a ; echo b"));
    expectThrows(AssertionError.class, () -> session.run("echo $(date)"));
    expectThrows(AssertionError.class, () -> session.run("echo > /dev/null"));
    AssertionError assignment =
        expectThrows(AssertionError.class, () -> session.run("SOLR_LOG_LEVEL=DEBUG echo hi"));
    assertTrue(assignment.getMessage(), assignment.getMessage().contains("env(\"SOLR_LOG_LEVEL\""));
  }

  @Test
  public void singleQuotesAreRejectedBecauseCmdHasNoSuchQuoting() {
    AssertionError failure =
        expectThrows(AssertionError.class, () -> session.run("echo 'hello world'"));
    assertTrue(failure.getMessage(), failure.getMessage().contains("single-quote quoting"));
  }

  @Test
  public void aMetacharacterInsideDoubleQuotesIsFine() {
    // Both shells protect it, so a URL with a query string must not trip the rails.
    session
        .run("echo \"http://localhost:${SOLR_PORT}/solr/x?q=*:*&rows=10\"")
        .assertSuccess()
        .assertContains("&rows=10");
  }

  // ------------------------------------------------------- output extraction

  @Test
  public void outputExtractionReplacesTheUnixTextTools() {
    var result =
        session.script(
            "printf 'alpha 1\\nbeta 2\\ngamma 3\\n'", "(echo alpha 1&echo beta 2&echo gamma 3)");
    result.assertSuccess().assertLineCount(3);
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
  public void anHttpsRequestThatCannotConnectIsReportedAsAFailureNotAStatus() {
    // Nothing is listening on the block this session claimed, which is the shape of the BATS
    // 'run ! curl https://...' line: no status, and the reason readable in the body.
    SolrCliHelpers.HttpResult result =
        session.helpers().https(session.ports().solr(), "/solr/admin/info/system").get();
    result.assertConnectionFailed();
    assertTrue(result.body(), result.body().contains("Connect"));
  }
}
