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

import static org.apache.solr.handler.ReplicationTestHelper.SolrInstance;
import static org.apache.solr.handler.ReplicationTestHelper.assertVersions;
import static org.apache.solr.handler.ReplicationTestHelper.createAndStartJetty;
import static org.apache.solr.handler.ReplicationTestHelper.index;
import static org.apache.solr.handler.ReplicationTestHelper.invokeReplicationCommand;
import static org.apache.solr.handler.ReplicationTestHelper.numFound;
import static org.apache.solr.handler.ReplicationTestHelper.rQuery;

import java.io.IOException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test for HealthCheckHandler in legacy mode */
@SuppressSSL // Currently, unknown why SSL does not work with this test
public class TestHealthCheckHandlerLegacyMode extends SolrTestCaseJ4 {
  SolrClient leaderClientHealthCheck, followerClientHealthCheck;
  JettySolrRunner leaderJetty, followerJetty;
  SolrClient leaderClient, followerClient;
  ReplicationTestHelper.SolrInstance leader = null, follower = null;

  private static final String context = "/solr";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    systemSetPropertySolrDisableUrlAllowList("true");

    leader =
        new ReplicationTestHelper.SolrInstance(
            createTempDir("solr-instance").toFile(), "leader", null);
    leader.setUp();
    leaderJetty = ReplicationTestHelper.createAndStartJetty(leader);
    leaderClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(leaderJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);
    leaderClientHealthCheck =
        ReplicationTestHelper.createNewSolrClient(buildUrl(leaderJetty.getLocalPort(), context));

    follower =
        new SolrInstance(
            createTempDir("solr-instance").toFile(), "follower", leaderJetty.getLocalPort());
    follower.setUp();
    followerJetty = createAndStartJetty(follower);
    followerClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(followerJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME);
    followerClientHealthCheck =
        ReplicationTestHelper.createNewSolrClient(buildUrl(followerJetty.getLocalPort(), context));

    System.setProperty("solr.indexfetcher.sotimeout2", "45000");
  }

  public void clearIndexWithReplication() throws Exception {
    if (numFound(ReplicationTestHelper.query("*:*", leaderClient)) != 0) {
      leaderClient.deleteByQuery("*:*");
      leaderClient.commit();
      // wait for replication to sync & verify
      assertEquals(0, numFound(rQuery(0, "*:*", followerClient)));
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (null != leaderJetty) {
      leaderJetty.stop();
      leaderJetty = null;
    }
    if (null != followerJetty) {
      followerJetty.stop();
      followerJetty = null;
    }
    if (null != leaderClient) {
      leaderClient.close();
      leaderClient = null;
    }
    if (null != followerClient) {
      followerClient.close();
      followerClient = null;
    }
    if (null != leaderClientHealthCheck) {
      leaderClientHealthCheck.close();
      leaderClientHealthCheck = null;
    }

    if (null != followerClientHealthCheck) {
      followerClientHealthCheck.close();
      followerClientHealthCheck = null;
    }
    System.clearProperty("solr.indexfetcher.sotimeout");
  }

  @Test
  // keep this
  public void doTestHealthCheckWithReplication() throws Exception {
    int nDocs = 500;

    TestInjection.delayBeforeFollowerCommitRefresh = random().nextInt(10);

    // stop replication so that the follower doesn't pull the index
    invokeReplicationCommand(
        buildUrl(followerJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME,
        "disablepoll");

    // create multiple commits
    int docsAdded = 0;
    for (int i = 0; docsAdded < nDocs / 2; i++, docsAdded++) {
      index(leaderClient, "id", i, "name", "name = " + i);
      if (i % 10 == 0) {
        leaderClient.commit();
      }
    }

    leaderClient.commit();

    assertNumFoundWithQuery(leaderClient, docsAdded);

    // ensure that the leader is always happy
    // first try without specifying maxGenerationLag lag
    ModifiableSolrParams solrParamsLeaderHealthCheck = new ModifiableSolrParams();
    HealthCheckRequest healthCheckRequestLeader = new HealthCheckRequest();
    assertEquals(
        CommonParams.OK,
        healthCheckRequestLeader
            .process(leaderClientHealthCheck)
            .getResponse()
            .get(CommonParams.STATUS));

    // now try adding maxGenerationLag request param
    solrParamsLeaderHealthCheck.add(HealthCheckRequest.PARAM_MAX_GENERATION_LAG, "2");
    assertEquals(
        CommonParams.OK,
        healthCheckRequestLeader
            .process(leaderClientHealthCheck)
            .getResponse()
            .get(CommonParams.STATUS));

    // follower should report healthy if maxGenerationLag is not specified
    HealthCheckRequest healthCheckRequestFollower = new HealthCheckRequest();
    assertEquals(
        CommonParams.OK,
        healthCheckRequestFollower
            .process(followerClientHealthCheck)
            .getResponse()
            .get(CommonParams.STATUS));

    // ensure follower is unhealthy when maxGenerationLag is specified
    // ModifiableSolrParams params = new ModifiableSolrParams();
    healthCheckRequestFollower = new HealthCheckRequest();
    healthCheckRequestFollower.setMaxGenerationLag(2);
    assertEquals(
        CommonParams.FAILURE,
        healthCheckRequestFollower
            .process(followerClientHealthCheck)
            .getResponse()
            .get(CommonParams.STATUS));

    // enable polling, force replication and ensure that the follower is healthy
    // invokeReplicationCommand(buildUrl(followerJetty.getLocalPort(), context) + "/" +
    // DEFAULT_TEST_CORENAME, "enablepoll");
    pullFromTo(leaderJetty, followerJetty);

    // check replicated and is healthy
    assertNumFoundWithQuery(followerClient, docsAdded);
    assertEquals(
        CommonParams.OK,
        healthCheckRequestFollower
            .process(followerClientHealthCheck)
            .getResponse()
            .get(CommonParams.STATUS));
    assertVersions(leaderClient, followerClient);

    // index more docs on the leader
    for (int i = docsAdded; docsAdded < nDocs; i++, docsAdded++) {
      index(leaderClient, "id", i, "name", "name = " + i);
      if (i % 10 == 0) {
        leaderClient.commit();
      }
    }
    leaderClient.commit();

    assertNumFoundWithQuery(leaderClient, docsAdded);

    // we have added docs to the leader and polling is disabled, this should fail
    healthCheckRequestFollower = new HealthCheckRequest();
    healthCheckRequestFollower.setMaxGenerationLag(2);
    assertEquals(
        CommonParams.FAILURE,
        healthCheckRequestFollower
            .process(followerClientHealthCheck)
            .getResponse()
            .get(CommonParams.STATUS));

    // force replication and ensure that the follower is healthy
    pullFromTo(leaderJetty, followerJetty);
    assertEquals(
        CommonParams.OK,
        healthCheckRequestFollower
            .process(followerClientHealthCheck)
            .getResponse()
            .get(CommonParams.STATUS));
    assertNumFoundWithQuery(followerClient, docsAdded);
    assertVersions(leaderClient, followerClient);
  }

  public static void pullFromTo(JettySolrRunner srcSolr, JettySolrRunner destSolr)
      throws IOException {
    String srcUrl = buildUrl(srcSolr.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME;
    String destUrl = buildUrl(destSolr.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME;
    ReplicationTestHelper.pullFromTo(srcUrl, destUrl);
  }

  private void assertNumFoundWithQuery(SolrClient client, int nDocs) throws Exception {
    NamedList<Object> queryRsp = rQuery(nDocs, "*:*", client);
    assertEquals(nDocs, numFound(queryRsp));
  }
}
