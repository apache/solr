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
package org.apache.solr.webapp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;

/**
 * Tests the Replication screen on a standalone leader/follower pair: the follower's screen shows
 * the leader info, and the polling and replicate-now actions work.
 */
public class AdminUiReplicationStandaloneTest extends AdminUiStandaloneTestBase {

  private static final String CORE = "collection1";
  private static final Path REPLICATION_CONF =
      ExternalPaths.SOURCE_HOME.resolve("core/src/test-files/solr/collection1/conf");

  private static JettySolrRunner leaderJetty;

  @BeforeClass
  public static void startLeaderAndFollower() throws Exception {
    // sets the solr.tests.* index-config properties the test solrconfigs require
    newRandomConfig();
    // the follower's leaderUrl is not covered by the URL allow-list
    System.setProperty(AllowListUrlChecker.ENABLE_URL_ALLOW_LIST, "false");

    Path leaderHome = buildReplicationHome("solrconfig-leader.xml", 0);
    leaderJetty = new JettySolrRunner(leaderHome.toString(), JettyConfig.builder().build());
    leaderJetty.start();

    Path followerHome = buildReplicationHome("solrconfig-follower.xml", leaderJetty.getLocalPort());
    standaloneJetty = startStandaloneJetty(followerHome);
    baseUrl = standaloneJetty.getBaseUrl().toString();
  }

  @AfterClass
  public static void stopLeaderAndFollower() throws Exception {
    if (standaloneJetty != null) {
      standaloneJetty.stop();
      standaloneJetty = null;
    }
    if (leaderJetty != null) {
      leaderJetty.stop();
      leaderJetty = null;
    }
  }

  @Test
  public void testReplicationScreenAndActions() throws Exception {
    openPage(CORE + "/replication", By.id("replication"));
    // the follower screen shows its own and the leader's index version info
    waitForPageContains("Version");
    // scraping the rendered DOM for the leader's port is flaky: the "leader url:" row
    // can lag the rest of the screen's data on the very first load. Assert against the
    // API response instead, like the isPollingDisabled/followerNumDocs checks below do.
    waitUntil(
        "follower should report the leader's url",
        () -> followerDetail("leaderUrl").contains(":" + leaderJetty.getLocalPort()));

    // disable polling so replication only happens on demand
    click(By.cssSelector("#replication button.disable-polling"));
    waitUntil(
        "polling should be disabled", () -> "true".equals(followerDetail("isPollingDisabled")));

    // index documents on the leader; the follower does not poll them
    try (var client = leaderJetty.newClient()) {
      for (int i = 1; i <= 2; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "repl-doc-" + i);
        doc.addField("name", "replicated");
        client.add(CORE, doc);
      }
      client.commit(CORE);
    }

    // replicate on demand and watch the docs arrive on the follower
    click(By.cssSelector("#replication button.replicate-now"));
    waitUntil("follower should receive the docs after replicate-now", () -> followerNumDocs() == 2);

    // re-enable polling
    click(By.cssSelector("#replication button.enable-polling"));
    waitUntil(
        "polling should be enabled again",
        () -> "false".equals(followerDetail("isPollingDisabled")));
    assertNoSevereConsoleErrors();
  }

  /** Builds a home with one core configured from the given test solrconfig variant. */
  private static Path buildReplicationHome(String solrconfigName, int leaderPort)
      throws IOException {
    Path home = createTempDir("replication-home");
    Files.copy(
        ExternalPaths.SOURCE_HOME.resolve("core/src/test-files/solr/solr.xml"),
        home.resolve("solr.xml"));
    Path confDir = home.resolve(CORE).resolve("conf");
    Files.createDirectories(confDir);
    String solrconfig = Files.readString(REPLICATION_CONF.resolve(solrconfigName));
    solrconfig =
        solrconfig
            .replace("TEST_PORT", Integer.toString(leaderPort))
            .replace("COMPRESSION", "internal");
    Files.writeString(confDir.resolve("solrconfig.xml"), solrconfig);
    Files.copy(REPLICATION_CONF.resolve("schema-replication1.xml"), confDir.resolve("schema.xml"));
    Files.copy(
        REPLICATION_CONF.resolve("solrconfig.snippet.randomindexconfig.xml"),
        confDir.resolve("solrconfig.snippet.randomindexconfig.xml"));
    Properties props = new Properties();
    props.setProperty("name", CORE);
    writeCoreProperties(home.resolve(CORE), props, CORE);
    return home;
  }

  /** Reads a detail from the follower section of the replication details API. */
  private String followerDetail(String key) {
    try {
      NamedList<Object> response =
          adminApi("/" + CORE + "/replication", params("command", "details"));
      Object value = response._get(List.of("details", "follower", key), null);
      return value == null ? "" : value.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private long followerNumDocs() {
    try (var client = standaloneJetty.newClient()) {
      return client.query(CORE, new SolrQuery("*:*")).getResults().getNumFound();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
