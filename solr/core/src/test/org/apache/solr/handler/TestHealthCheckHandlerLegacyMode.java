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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.HealthCheckHandler;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.common.params.CommonParams.HEALTH_CHECK_HANDLER_PATH;

/**
 * Test for HealthCheckHandler in legacy mode
 *
 *
 */
@Slow
@SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestHealthCheckHandlerLegacyMode extends AbstractReplicationTestBase {

  HttpSolrClient leaderClientHealthCheck, followerClientHealthCheck;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    leaderClientHealthCheck = createNewSolrClient(leaderJetty.getLocalPort(), false);
  }

  public void clearIndexWithReplication() throws Exception {
    if (numFound(query("*:*", leaderClient)) != 0) {
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

    if(null != leaderClientHealthCheck)  {
      leaderClientHealthCheck.close();
      leaderClientHealthCheck = null;
    }

    if(null != followerClientHealthCheck)  {
      followerClientHealthCheck.close();
      followerClientHealthCheck = null;
    }
  }

  @Test
  // keep this
  public void doTestHealthCheckWithReplication() throws Exception {

    TestInjection.delayBeforeFollowerCommitRefresh = random().nextInt(10);

    //clearIndexWithReplication();

    nDocs--;
    // create multiple commits
    int docsAdded = 0;
    for (int i = 0; docsAdded < nDocs / 2; i++, docsAdded++) {
      index(leaderClient, "id", i, "name", "name = " + i);
      if( i % 10 == 0) {
        leaderClient.commit();
      }
    }

    leaderClient.commit();

    assertNumFoundWithQuery(leaderClient, docsAdded);

    //ensure that the leader is always happy
    ModifiableSolrParams solrParamsLeaderHealthCheck = new ModifiableSolrParams();
    SolrRequest healthCheckRequestLeader = new GenericSolrRequest(SolrRequest.METHOD.GET, HEALTH_CHECK_HANDLER_PATH, solrParamsLeaderHealthCheck);
    assertEquals(CommonParams.OK, healthCheckRequestLeader.process(leaderClientHealthCheck).getResponse().get(CommonParams.STATUS));

    // now try adding generation lag
    solrParamsLeaderHealthCheck.add(HealthCheckHandler.PARAM_MAX_GENERATION_LAG, "2");
    assertEquals(CommonParams.OK, healthCheckRequestLeader.process(leaderClientHealthCheck).getResponse().get(CommonParams.STATUS));
    // now ry with always be in sync
    solrParamsLeaderHealthCheck.add(HealthCheckHandler.PARAM_REQUIRE_ALWAYS_BE_IN_SYNC, "true");
    assertEquals(CommonParams.OK, healthCheckRequestLeader.process(leaderClientHealthCheck).getResponse().get(CommonParams.STATUS));


    follower = new SolrInstance(createTempDir("solr-instance").toFile(), "follower", leaderJetty.getLocalPort());
    follower.setUp();
    followerJetty = createAndStartJetty(follower);
    followerClient = createNewSolrClient(followerJetty.getLocalPort(), true);
    followerClientHealthCheck = createNewSolrClient(followerJetty.getLocalPort(), false);

    // stop replication so that the follower doesn't pull the index
    invokeReplicationCommand(followerJetty.getLocalPort(), "disablepoll");

    //follower should report health if maxGenerationLag is not specified
    SolrRequest healthCheckRequestFolllower = new GenericSolrRequest(SolrRequest.METHOD.GET, HEALTH_CHECK_HANDLER_PATH, new ModifiableSolrParams());
    assertEquals(CommonParams.OK, healthCheckRequestFolllower.process(followerClientHealthCheck).getResponse().get(CommonParams.STATUS));

    //ensure follower is unhealthy
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(HealthCheckHandler.PARAM_MAX_GENERATION_LAG, "2");
    healthCheckRequestFolllower = new GenericSolrRequest(SolrRequest.METHOD.GET, HEALTH_CHECK_HANDLER_PATH, params);
    assertEquals(CommonParams.FAILURE, healthCheckRequestFolllower.process(followerClientHealthCheck).getResponse().get(CommonParams.STATUS));

    //enable polling, force replication and ensure that the follower is healthy
    pullFromLeaderToFollower();

    // check replicated and is healthy
    assertNumFoundWithQuery(followerClient, docsAdded);
    assertEquals(CommonParams.OK, healthCheckRequestFolllower.process(followerClientHealthCheck).getResponse().get(CommonParams.STATUS));
    assertVersions(leaderClient, followerClient);

    //index more docs on the leader
    for (int i = docsAdded; docsAdded < nDocs; i++, docsAdded++) {
      index(leaderClient, "id", i, "name", "name = " + i);
      if( i % 10 == 0) {
        leaderClient.commit();
      }
    }
    leaderClient.commit();

    assertNumFoundWithQuery(leaderClient, docsAdded);

    // check without always be in sync. We synced at the start so the weak check should be OK
    assertEquals(CommonParams.OK, healthCheckRequestFolllower.process(followerClientHealthCheck).getResponse().get(CommonParams.STATUS));

    // check with always be in sync, we have added docs to the leader and polling is disabled, this should fail
    params.add(HealthCheckHandler.PARAM_REQUIRE_ALWAYS_BE_IN_SYNC, "true");
    healthCheckRequestFolllower = new GenericSolrRequest(SolrRequest.METHOD.GET, HEALTH_CHECK_HANDLER_PATH, params);
    assertEquals(CommonParams.FAILURE, healthCheckRequestFolllower.process(followerClientHealthCheck).getResponse().get(CommonParams.STATUS));

    //force replication and ensure that the follower is healthy
    pullFromLeaderToFollower();
    assertEquals(CommonParams.OK, healthCheckRequestFolllower.process(followerClientHealthCheck).getResponse().get(CommonParams.STATUS));
    assertNumFoundWithQuery(followerClient, docsAdded);
    assertVersions(leaderClient, followerClient);
  }

  private void assertNumFoundWithQuery(HttpSolrClient client, int nDocs) throws Exception {
    NamedList queryRsp = rQuery(nDocs, "*:*", client);
    SolrDocumentList leaderQueryResult = (SolrDocumentList) queryRsp.get("response");
    assertEquals(nDocs, numFound(queryRsp));
  }
}
