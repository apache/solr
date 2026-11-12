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

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.junit.Test;

/**
 * Port of {@code test_help.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs: the BATS {@code skip} guarding the not-yet-migrated {@code restart} help
 * becomes {@link EnabledOnCli}, and {@code run -1} becomes {@code runFail}. Nothing here starts
 * Solr, so there is no {@code setup_file} equivalent.
 */
public class HelpTest extends SolrShellTestCase {

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18649")
  public void solrHelpFlagPrintsHelp() {
    run("solr --help")
        .assertContainsForCli("Usage: solr COMMAND OPTIONS", "usage: bin/solr")
        .assertNotContains("ERROR");
  }

  @Test
  public void solrWithNoFlagsPrintsHelp() {
    runFail("solr")
        .assertExitCode(1)
        .assertContainsForCli("Usage: solr COMMAND OPTIONS", "usage: bin/solr")
        .assertNotContains("ERROR");
  }

  @Test
  public void startHelpFlagPrintsHelp() {
    run("solr start --help")
        .assertContainsForCli("Usage: solr start", "usage: bin/solr start")
        .assertNotContains("ERROR");
  }

  @Test
  public void startHFlagPrintsHelp() {
    run("solr start -h")
        .assertContainsForCli("Usage: solr start", "usage: bin/solr start")
        .assertNotContains("ERROR: Hostname is required when using the -h option!");
  }

  @Test
  public void stopHelpFlagPrintsHelp() {
    run("solr stop --help")
        .assertContainsForCli("Usage: solr stop", "usage: bin/solr stop")
        .assertNotContains("ERROR");
  }

  @Test
  public void restartHelpFlagPrintsHelp() {
    run("solr restart --help")
        .assertContainsForCli("Usage: solr restart", "usage: bin/solr restart")
        .assertNotContains("ERROR");
  }

  @Test
  public void statusHelpFlagPrintsHelp() {
    run("solr status --help").assertContains("usage: bin/solr status").assertNotContains("ERROR");
  }

  @Test
  public void healthcheckHelpFlagPrintsHelp() {
    run("solr healthcheck --help")
        .assertContains("usage: bin/solr healthcheck")
        .assertNotContains("ERROR");
  }

  @Test
  public void createHelpFlagPrintsHelp() {
    run("solr create --help").assertContains("usage: bin/solr create").assertNotContains("ERROR");
  }

  @Test
  public void deleteHelpFlagPrintsHelp() {
    run("solr delete -h").assertContains("usage: bin/solr delete").assertNotContains("ERROR");
  }

  @Test
  public void zkHelpFlagPrintsHelp() {
    run("solr zk --help")
        .assertContains("usage:")
        .assertContainsForCli("bin/solr zk ls", "usage: bin/solr zk")
        .assertNotContains("ERROR");
  }

  @Test
  public void authHelpFlagPrintsHelp() {
    run("solr auth --help")
        .assertContainsForCli("bin/solr auth enable", "usage: bin/solr auth")
        .assertNotContains("ERROR");
  }

  @Test
  public void assertHelpFlagPrintsHelp() {
    run("solr assert --help").assertContains("usage: bin/solr assert").assertNotContains("ERROR");
  }

  @Test
  public void postHelpFlagPrintsHelp() {
    run("solr post --help").assertContains("usage: bin/solr post").assertNotContains("ERROR");
  }
}
