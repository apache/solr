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

import static org.apache.solr.common.params.CommonParams.JAVABIN;
import static org.apache.solr.handler.ReplicationHandler.CMD_DISABLE_POLL;
import static org.apache.solr.handler.ReplicationHandler.CMD_FETCH_INDEX;
import static org.apache.solr.handler.ReplicationHandler.COMMAND;
import static org.apache.solr.handler.ReplicationTestHelper.createAndStartJetty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressSSL
public class TestUserManagedReplicationWithAuth extends SolrTestCaseJ4 {
  JettySolrRunner leaderJetty, followerJetty, followerJettyWithAuth;
  SolrClient leaderClient, followerClient, followerClientWithAuth;
  ReplicationTestHelper.SolrInstance leader = null, follower = null, followerWithAuth = null;

  private static String user = "solr";
  private static String pass = "SolrRocks";
  private static String securityJson =
      "{\n"
          + "\"authentication\":{ \n"
          + "   \"blockUnknown\": true, \n"
          + "   \"class\":\"solr.BasicAuthPlugin\",\n"
          + "   \"credentials\":{\"solr\":\"IV0EHq1OnNrj6gvRCwvFwTrZ1+z1oBbnQdiVC3otuq0= Ndd7LKvVBAaZIF0QAVi1ekCfAJXr1GGfLtRUXhgrF8c=\"}, \n"
          + "   \"realm\":\"My Solr users\", \n"
          + "   \"forwardCredentials\": false \n"
          + "},\n"
          + "\"authorization\":{\n"
          + "   \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n"
          + "   \"permissions\":[{\"name\":\"security-edit\",\n"
          + "      \"role\":\"admin\"}],\n"
          + "   \"user-role\":{\"solr\":\"admin\"}\n"
          + "}}";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    systemSetPropertySolrDisableUrlAllowList("true");
    // leader with Basic auth enabled via security.json
    leader =
        new ReplicationTestHelper.SolrInstance(
            createTempDir("solr-instance").toFile(), "leader", null);
    leader.setUp();
    // Configuring basic auth for Leader
    Path solrLeaderHome = Path.of(leader.getHomeDir());
    Files.write(
        solrLeaderHome.resolve("security.json"), securityJson.getBytes(StandardCharsets.UTF_8));
    leaderJetty = ReplicationTestHelper.createAndStartJetty(leader);
    leaderClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(leaderJetty.getLocalPort()), DEFAULT_TEST_CORENAME);

    // follower with no basic auth credentials for leader configured.
    follower =
        new ReplicationTestHelper.SolrInstance(
            createTempDir("solr-instance").toFile(), "follower", leaderJetty.getLocalPort());
    follower.setUp();
    followerJetty = createAndStartJetty(follower);
    followerClient =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(followerJetty.getLocalPort()), DEFAULT_TEST_CORENAME);

    // follower with basic auth credentials for leader configured in solrconfig.xml.
    followerWithAuth =
        new ReplicationTestHelper.SolrInstance(
            createTempDir("solr-instance").toFile(), "follower-auth", leaderJetty.getLocalPort());
    followerWithAuth.setUp();
    followerJettyWithAuth = createAndStartJetty(followerWithAuth);
    followerClientWithAuth =
        ReplicationTestHelper.createNewSolrClient(
            buildUrl(followerJettyWithAuth.getLocalPort()), DEFAULT_TEST_CORENAME);
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
    if (null != followerJettyWithAuth) {
      followerJettyWithAuth.stop();
      followerJettyWithAuth = null;
    }
    if (null != leaderClient) {
      leaderClient.close();
      leaderClient = null;
    }
    if (null != followerClient) {
      followerClient.close();
      followerClient = null;
    }
    if (null != followerClientWithAuth) {
      followerClientWithAuth.close();
      followerClientWithAuth = null;
    }
  }

  private <T extends SolrRequest<? extends SolrResponse>> T withBasicAuth(T req) {
    req.setBasicAuthCredentials(user, pass);
    return req;
  }

  @Test
  public void doTestManualFetchIndexWithAuthEnabled() throws Exception {
    disablePoll(followerJetty, followerClient);
    int nDocs = 500;
    int docsAdded = 0;

    UpdateRequest commitReq = new UpdateRequest();
    withBasicAuth(commitReq);
    for (int i = 0; docsAdded < nDocs / 2; i++, docsAdded++) {
      SolrInputDocument doc = new SolrInputDocument();
      String[] fields = {"id", i + "", "name", "name = " + i};
      for (int j = 0; j < fields.length; j += 2) {
        doc.addField(fields[j], fields[j + 1]);
      }
      UpdateRequest req = new UpdateRequest();
      withBasicAuth(req).add(doc);
      req.process(leaderClient, DEFAULT_TEST_CORENAME);
      if (i % 10 == 0) {
        commitReq.commit(leaderClient, DEFAULT_TEST_CORENAME);
      }
    }
    commitReq.commit(leaderClient, DEFAULT_TEST_CORENAME);

    assertEquals(
        docsAdded,
        queryWithBasicAuth(leaderClient, new SolrQuery("*:*")).getResults().getNumFound());

    // Without Auth credentials fetchIndex will fail
    pullIndexFromTo(leaderJetty, followerJetty, false);
    assertNotEquals(
        docsAdded,
        queryWithBasicAuth(followerClient, new SolrQuery("*:*")).getResults().getNumFound());

    // With Auth credentials
    pullIndexFromTo(leaderJetty, followerJetty, true);
    assertEquals(
        docsAdded,
        queryWithBasicAuth(followerClient, new SolrQuery("*:*")).getResults().getNumFound());
  }

  @Test
  public void doTestAutoReplicationWithAuthEnabled() throws Exception {
    int nDocs = 250;
    UpdateRequest commitReq = new UpdateRequest();
    withBasicAuth(commitReq);
    for (int i = 0; i < nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      String[] fields = {"id", i + "", "name", "name = " + i};
      for (int j = 0; j < fields.length; j += 2) {
        doc.addField(fields[j], fields[j + 1]);
      }
      UpdateRequest req = new UpdateRequest();
      withBasicAuth(req).add(doc);
      req.process(leaderClient, DEFAULT_TEST_CORENAME);
      if (i % 10 == 0) {
        commitReq.commit(leaderClient, DEFAULT_TEST_CORENAME);
      }
    }
    commitReq.commit(leaderClient, DEFAULT_TEST_CORENAME);
    // wait for followers to fetchIndex
    Thread.sleep(5000);
    // follower with auth should be healthy
    HealthCheckRequest healthCheckRequestFollower = new HealthCheckRequest();
    healthCheckRequestFollower.setMaxGenerationLag(2);
    assertEquals(
        CommonParams.OK,
        healthCheckRequestFollower
            .process(followerClientWithAuth)
            .getResponse()
            .get(CommonParams.STATUS));
    // follower with auth should be unhealthy
    healthCheckRequestFollower = new HealthCheckRequest();
    healthCheckRequestFollower.setMaxGenerationLag(2);
    assertEquals(
        CommonParams.FAILURE,
        healthCheckRequestFollower.process(followerClient).getResponse().get(CommonParams.STATUS));
  }

  private QueryResponse queryWithBasicAuth(SolrClient client, SolrQuery q)
      throws IOException, SolrServerException {
    return withBasicAuth(new QueryRequest(q)).process(client);
  }

  private void disablePoll(JettySolrRunner Jetty, SolrClient solrClient)
      throws SolrServerException, IOException {
    ModifiableSolrParams disablePollParams = new ModifiableSolrParams();
    disablePollParams.set(COMMAND, CMD_DISABLE_POLL);
    disablePollParams.set(CommonParams.WT, JAVABIN);
    disablePollParams.set(CommonParams.QT, ReplicationHandler.PATH);
    QueryRequest req = new QueryRequest(disablePollParams);
    withBasicAuth(req);
    req.setBasePath(buildUrl(Jetty.getLocalPort()));

    solrClient.request(req, DEFAULT_TEST_CORENAME);
  }

  private void pullIndexFromTo(
      JettySolrRunner srcSolr, JettySolrRunner destSolr, boolean authEnabled)
      throws SolrServerException, IOException {
    String srcUrl = buildUrl(srcSolr.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME;
    String destUrl = buildUrl(destSolr.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME;
    QueryRequest req = getQueryRequestForFetchIndex(authEnabled, srcUrl);
    req.setBasePath(buildUrl(destSolr.getLocalPort()));
    followerClient.request(req, DEFAULT_TEST_CORENAME);
  }

  private QueryRequest getQueryRequestForFetchIndex(boolean authEnabled, String srcUrl) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(COMMAND, CMD_FETCH_INDEX);
    solrParams.set(CommonParams.WT, JAVABIN);
    solrParams.set(CommonParams.QT, ReplicationHandler.PATH);
    solrParams.set("leaderUrl", srcUrl);
    solrParams.set("wait", "true");
    if (authEnabled) {
      solrParams.set("httpBasicAuthUser", user);
      solrParams.set("httpBasicAuthPassword", pass);
    }
    QueryRequest req = new QueryRequest(solrParams);
    return req;
  }
}
