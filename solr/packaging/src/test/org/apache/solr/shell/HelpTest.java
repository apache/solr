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

import org.junit.Test;

/** Port of {@code test_help.bats}. */
public class HelpTest extends SolrCatsTestCase {

  @Test
  public void solrHelpFlagPrintsHelp() {
    for (String flag : new String[] {"--help", "-h"}) {
      run("solr " + flag)
          .assertContains(forCli("Usage: solr COMMAND OPTIONS", "usage: bin/solr"))
          .assertNotContains("ERROR");
    }
  }

  @Test
  public void solrWithNoFlagsPrintsHelp() {
    runFail("solr")
        .assertExitCode(2)
        .assertContains(forCli("Usage: solr COMMAND OPTIONS", "usage: bin/solr"))
        .assertNotContains("ERROR");
  }

  @Test
  public void zkRequiresASubcommand() {
    checkParentHelp("zk", "bin/solr zk ls", "usage: bin/solr zk");
  }

  @Test
  public void snapshotRequiresASubcommand() {
    checkParentHelp("snapshot", "snapshot-create", "usage: bin/solr snapshot");
  }

  @Test
  public void packageRequiresASubcommand() {
    checkParentHelp("package", "Package Manager", "usage: bin/solr package");
  }

  @Test
  public void helpPropagatesAJvmStartupFailure() {
    env("SOLR_TOOL_OPTS", "-XX:SolrInvalidOptionForTest");
    runFail("solr --help").assertExitCode(1).assertContains("Unrecognized VM option");
  }

  @Test
  public void startHelpFlagPrintsHelp() {
    run("solr start --help")
        .assertContains(forCli("Usage: solr start", "usage: bin/solr start"))
        .assertNotContains("ERROR");
  }

  @Test
  public void startHFlagPrintsHelp() {
    run("solr start -h")
        .assertContains(forCli("Usage: solr start", "usage: bin/solr start"))
        .assertNotContains("ERROR: Hostname is required when using the -h option!");
  }

  @Test
  public void stopHelpFlagPrintsHelp() {
    run("solr stop --help")
        .assertContains(forCli("Usage: solr stop", "usage: bin/solr stop"))
        .assertNotContains("ERROR");
  }

  @Test
  public void restartHelpFlagPrintsHelp() {
    run("solr restart --help")
        .assertContains(forCli("Usage: solr restart", "usage: bin/solr restart"))
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
        .assertContains(forCli("bin/solr zk ls", "usage: bin/solr zk"))
        .assertNotContains("ERROR");
  }

  @Test
  public void authHelpFlagPrintsHelp() {
    run("solr auth --help")
        .assertContains(forCli("bin/solr auth enable", "usage: bin/solr auth"))
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

  private void checkParentHelp(String parent, String commonsMarker, String picocliMarker) {
    runFail("solr " + parent)
        .assertExitCode(2)
        .assertContains(forCli(commonsMarker, picocliMarker))
        .assertNotContains("ERROR");
    for (String flag : new String[] {"--help", "-h"}) {
      run("solr " + parent + " " + flag)
          .assertContains(forCli(commonsMarker, picocliMarker))
          .assertNotContains("ERROR");
    }
  }
}
