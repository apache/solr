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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.apache.solr.shell.exec.CommandResult;
import org.apache.solr.shell.exec.Shell;
import org.junit.Test;

/**
 * Port of {@code test_start_solr.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on. The
 * original starts Solr inside every test rather than in {@code setup_file}, so each test here does
 * too; the harness stops what a test started, which is the original's {@code teardown}. Where it
 * differs: {@code curl} becomes {@link #http}, {@code cat} becomes {@link #read}, the {@code
 * timeout} utility becomes an explicit command timeout, {@code RMI_PORT} is a port this class owns
 * rather than the hardcoded 65535, and the stop-arg test is split per shell because the two scripts
 * reject the argument with different wording.
 */
public class StartSolrTest extends SolrShellTestCase {

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18647")
  public void solr11740CheckSolrStopConnection() {
    run("solr start");
    run("solr start --user-managed -p ${SOLR2_PORT}");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --cloud http://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000");
    run("solr assert --not-cloud http://localhost:${SOLR2_PORT} --timeout 5000");

    // The original wrapped this in 'bash -c ... 2>&1' only to merge the streams; the harness
    // already merges them.
    CommandResult stopped = run("solr stop --all").assertNotContains("forcefully killing");
    // The graceful-stop wording is bash's alone - solr.cmd says "Stopping Solr process N running
    // on port P" and never printed this - so only assert it where it exists. An earlier version of
    // this port asserted it on both shells, which is a test bug, not a script one.
    if (cli.shell() == Shell.BASH) {
      stopped.assertContains("Sending stop command");
    }

    // What SOLR-11740 is actually about, and true of both shells: --all stopped *both* nodes.
    // The refutation above would hold even if nothing had been stopped at all.
    run("solr assert --not-started http://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --not-started http://localhost:${SOLR2_PORT} --timeout 5000");
  }

  @Test
  public void stopCommandForSinglePort() {
    run("solr start");
    run("solr start -p ${SOLR2_PORT}");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --started http://localhost:${SOLR2_PORT} --timeout 5000");

    run("solr assert --cloud http://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --cloud http://localhost:${SOLR2_PORT} --timeout 5000");

    run("solr stop -p ${SOLR2_PORT}");
    run("solr assert --not-started http://localhost:${SOLR2_PORT} --timeout 5000");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");
  }

  // For start/stop/restart we parse the args separate from picking the command, which means you
  // don't get an error message for passing a start arg, like --jvm-opts, to a stop command. Make
  // sure that passing a non flag option (i.e. --jvm-opts "blah") doesn't hang the stop command:
  // the harness kills a command that overruns its timeout and fails the test, which is what the
  // original's 'timeout 2' did (and it no longer has to skip when that utility is missing).

  @Test
  @EnabledOnShell(
      Shell.BASH) // bin/solr parses the start args for stop too, so it names the option.
  public void checkStopCommandDoesntHang() {
    runFail(Duration.ofSeconds(30), "solr stop --jvm-opts")
        .assertContains("ERROR: JVM options are required when using the --jvm-opts option!");
  }

  @Test
  @EnabledOnShell(Shell.CMD) // solr.cmd skips the start args for stop and rejects it as unknown.
  public void checkStopCommandDoesntHangOnCmd() {
    run(Duration.ofSeconds(30), "solr stop --jvm-opts")
        .assertContains("Invalid command-line option: --jvm-opts");
  }

  @Test
  public void solr16976SolrStartsWithRemoteJmxEnabled() {
    env("ENABLE_REMOTE_JMX_OPTS", "true");
    // The original hardcoded 65535 so the default RMI_PORT (SOLR_PORT+10000) could not exceed the
    // port range. A port this class owns does the same job and cannot collide with another class.
    env("RMI_PORT", Integer.toString(ports().solr3()));

    run("solr start");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");

    String consoleLog =
        read(harnessEnv.logsDir().resolve("solr-" + ports().solr() + "-console.log").toString());
    assertFalse(
        "console log reported a thread exception:\n" + consoleLog,
        consoleLog.contains("Exception in thread"));
  }

  @Test
  public void deprecatedSystemPropertiesConvertedToModernProperties() {
    run("solr start -Ddisable.config.edit=true");
    assertTrue(
        fileContains(
            harnessEnv.logsDir().resolve("solr.log").toString(),
            "Deprecated system property disable.config.edit has been replaced by"
                + " solr.api.config.edit.enabled"));
  }

  @Test
  public void startWithCustomJettyOptions() {
    env("ENABLE_REMOTE_JMX_OPTS", "true");
    env("RMI_PORT", Integer.toString(ports().solr3()));

    run("solr start --jettyconfig \"--module=server\"");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");
  }

  @Test
  public void webappIsDeployedAtTheSolrContext() {
    // Jetty 12.1 removed the directory-scanning deployer; the Solr webapp is now added
    // directly to the context collection in server/etc/jetty.xml. Verify it deploys at
    // /solr and that the context is bound to that path only (an unmapped path 404s).
    run("solr start");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");

    http("/solr/admin/info/system").assertStatus(200);
    http("/not-solr/admin/info/system").assertStatus(404);
  }

  @Test
  public void cFlagPrintsNoOpWarningAndStillStartsInCloudMode() {
    run("solr start -c")
        .assertContains("WARNING: -c/--cloud is a no-op. Solr starts in cloud mode by default.");

    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --cloud http://localhost:${SOLR_PORT} --timeout 5000");
  }

  @Test
  public void bootstrappingAConfigset() {
    // Verify the source configset directory exists. 'test -d' is a shell builtin, so this is the
    // one line that moves into Java.
    Path confdir =
        harnessEnv.solrTip().resolve("server/solr/configsets/sample_techproducts_configs/conf");
    assertTrue("missing configset directory " + confdir, Files.isDirectory(confdir));

    // Start Solr with bootstrap_confdir pointing to techproducts configset
    run(
        "solr start -Dbootstrap_confdir=\"${SOLR_TIP}/server/solr/configsets/"
            + "sample_techproducts_configs/conf\" -Dcollection.configName=techproducts");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");

    // Verify the techproducts configset was uploaded
    assertTrue(configExists("techproducts"));
  }
}
