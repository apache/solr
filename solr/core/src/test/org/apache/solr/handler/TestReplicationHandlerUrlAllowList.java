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
package org.apache.solr.handler;

import static org.hamcrest.CoreMatchers.containsString;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.ReplicationTestHelper.SolrInstance;
import org.apache.solr.security.AllowListUrlChecker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Non-nightly coverage for SOLR-18280. {@link TestReplicationHandler} is {@code @Nightly}; the
 * allow-list wiring lives in {@link ReplicationTestHelper#createAndStartJetty} and must be proven
 * without that annotation.
 */
@SuppressSSL
public class TestReplicationHandlerUrlAllowList extends SolrTestCaseJ4 {

  private String previousTestUrlAllowList;
  private String previousEnableUrlAllowList;

  @Before
  public void saveAllowListProperties() {
    previousTestUrlAllowList = System.getProperty(TEST_URL_ALLOW_LIST);
    previousEnableUrlAllowList = System.getProperty(AllowListUrlChecker.ENABLE_URL_ALLOW_LIST);
  }

  @After
  public void restoreAllowListProperties() {
    restoreProperty(TEST_URL_ALLOW_LIST, previousTestUrlAllowList);
    restoreProperty(AllowListUrlChecker.ENABLE_URL_ALLOW_LIST, previousEnableUrlAllowList);
  }

  private static void restoreProperty(String propertyName, String previousValue) {
    if (previousValue == null) {
      System.clearProperty(propertyName);
    } else {
      System.setProperty(propertyName, previousValue);
    }
  }

  @Test
  public void testReplicationFetchHonorsTestUrlAllowList() throws Exception {
    System.setProperty(AllowListUrlChecker.ENABLE_URL_ALLOW_LIST, "true");
    System.clearProperty(TEST_URL_ALLOW_LIST);

    SolrInstance leader = new SolrInstance(createTempDir("solr-instance"), "leader", null);
    leader.setUp();
    JettySolrRunner leaderJetty = ReplicationTestHelper.createAndStartJetty(leader);
    JettySolrRunner followerJetty = null;
    try {
      String leaderUrl = buildUrl(leaderJetty.getLocalPort());
      String leaderCoreUrl = leaderUrl + "/" + DEFAULT_TEST_CORENAME;
      System.setProperty(TEST_URL_ALLOW_LIST, leaderUrl);

      SolrInstance follower =
          new SolrInstance(createTempDir("solr-instance"), "follower", leaderJetty.getLocalPort());
      follower.setUp();
      followerJetty = ReplicationTestHelper.createAndStartJetty(follower);
      String followerUrl = buildUrl(followerJetty.getLocalPort());
      String followerCoreUrl = followerUrl + "/" + DEFAULT_TEST_CORENAME;
      try (SolrCore core = followerJetty.getCoreContainer().getCore(DEFAULT_TEST_CORENAME)) {
        assertEquals(follower.getDataDir(), core.getDataDir());
      }

      AllowListUrlChecker checker = followerJetty.getCoreContainer().getAllowListUrlChecker();
      assertTrue(checker.isEnabled());
      checker.checkAllowList(List.of(leaderUrl));

      try (SolrClient leaderClient =
              ReplicationTestHelper.createNewSolrClient(leaderUrl, DEFAULT_TEST_CORENAME);
          SolrClient followerClient =
              ReplicationTestHelper.createNewSolrClient(followerUrl, DEFAULT_TEST_CORENAME)) {
        ReplicationTestHelper.index(leaderClient, "id", "allow-list-1", "name", "allowed");
        leaderClient.commit();
        ReplicationTestHelper.pullFromTo(leaderCoreUrl, followerCoreUrl);

        NamedList<Object> response =
            ReplicationTestHelper.rQuery(1, "id:allow-list-1", followerClient);
        assertEquals(1L, ReplicationTestHelper.numFound(response));
      }
    } finally {
      if (followerJetty != null) {
        followerJetty.stop();
      }
      leaderJetty.stop();
    }
  }

  @Test
  public void testEmptyAllowListRejectsLeaderUrl() throws Exception {
    System.setProperty(AllowListUrlChecker.ENABLE_URL_ALLOW_LIST, "true");
    System.clearProperty(TEST_URL_ALLOW_LIST);

    SolrInstance instance = new SolrInstance(createTempDir("solr-instance"), "leader", null);
    instance.setUp();
    JettySolrRunner jetty = ReplicationTestHelper.createAndStartJetty(instance);
    try {
      AllowListUrlChecker checker = jetty.getCoreContainer().getAllowListUrlChecker();
      assertTrue(checker.isEnabled());
      SolrException denied =
          expectThrows(
              SolrException.class,
              () -> checker.checkAllowList(List.of("http://127.0.0.1:8983/solr/collection1")));
      assertThat(denied.getMessage(), containsString(AllowListUrlChecker.URL_ALLOW_LIST));
    } finally {
      jetty.stop();
    }
  }
}
