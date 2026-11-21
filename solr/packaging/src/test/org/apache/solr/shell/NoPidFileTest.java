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
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;

/**
 * Port of {@code test_no_pid_file.bats}.
 *
 * <p>Solr started in the foreground writes no PID file, which is the normal case in containers.
 * Hiding the PID file of a node started in the background reproduces that state.
 *
 * <p>Note that "stop --all" and a bare "stop" intentionally remain limited to nodes with a PID file
 * in SOLR_PID_DIR, so that they never reap Solr instances belonging to another installation.
 *
 * <p>The PID file name is resolved per platform: {@code bin/solr} writes {@code solr-<port>.pid}
 * while {@code bin\solr.cmd} writes {@code solr-<port>.port}. The refutation in {@link
 * #stopByPortWorksWithoutAPidFile} matches both scripts' wording, {@code bin\solr.cmd} phrasing the
 * not-found case differently.
 */
public class NoPidFileTest extends SolrCatsTestCase {

  /** Port of {@code start_solr_without_pid_file}. */
  private void startSolrWithoutPidFile() {
    run("solr start");
    run("solr assert --started http://localhost:${SOLR_PORT} --timeout 5000");

    Path pidFile = pidFile();
    assertTrue("the start script wrote no PID file at " + pidFile, Files.exists(pidFile));
    Path hidden = mkdirs("hidden-pid").resolve(pidFile.getFileName().toString());
    try {
      Files.move(pidFile, hidden);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not move " + pidFile + " aside", e);
    }
  }

  /**
   * The file the start script records this node in: {@code solr-<port>.pid} holding a PID on bash,
   * {@code solr-<port>.port} holding the port on cmd.
   */
  private Path pidFile() {
    Path pidForm = harnessEnv.pidDir().resolve("solr-" + ports().solr() + ".pid");
    return Files.exists(pidForm)
        ? pidForm
        : harnessEnv.pidDir().resolve("solr-" + ports().solr() + ".port");
  }

  @Test
  public void statusFindsARunningNodeThatHasNoPidFile() {
    startSolrWithoutPidFile();

    run("solr status").assertContains("running on port ${SOLR_PORT}");
  }

  @Test
  public void statusShortFindsARunningNodeThatHasNoPidFile() {
    startSolrWithoutPidFile();

    run("solr status --short").assertContains("http://localhost:${SOLR_PORT}/solr");
  }

  @Test
  public void stopByPortWorksWithoutAPidFile() {
    startSolrWithoutPidFile();

    run("solr stop -p ${SOLR_PORT}")

        // bin/solr says "No process found ...", bin\solr.cmd "No Solr found running on port ...".
        .assertNotMatches("No (process|Solr) found");
    run("solr assert --not-started http://localhost:${SOLR_PORT} --timeout 5000");
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18647")
  public void stopAllReportsWhenItStoppedNothing() {
    run("solr stop --all").assertContains("No Solr nodes found to stop.");
  }
}
