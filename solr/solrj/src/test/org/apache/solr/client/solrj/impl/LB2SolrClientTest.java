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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.jetty.LBJettySolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.LogListener;
import org.junit.BeforeClass;

/**
 * Integration test for {@link LBSolrClient}
 *
 * @since solr 1.4
 */
@LogLevel("org.apache.solr.client.solrj.impl=DEBUG")
public class LB2SolrClientTest extends SolrTestCaseJ4 {

  SolrInstance[] solr = new SolrInstance[3];

  // TODO: fix this test to not require FSDirectory

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    for (int i = 0; i < solr.length; i++) {
      solr[i] = new SolrInstance("solr/collection1" + i, createTempDir("instance-" + i), 0);
      solr[i].setUp();
      solr[i].startJetty();
      addDocs(solr[i]);
    }
  }

  private void addDocs(SolrInstance solrInstance) throws IOException, SolrServerException {
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", solrInstance.name);
      docs.add(doc);
    }
    SolrResponseBase resp;
    try (SolrClient client =
        getHttpSolrClient(solrInstance.getBaseUrl(), solrInstance.getDefaultCollection())) {
      resp = client.add(docs);
      assertEquals(0, resp.getStatus());
      resp = client.commit();
      assertEquals(0, resp.getStatus());
    }
  }

  @Override
  public void tearDown() throws Exception {
    for (SolrInstance aSolr : solr) {
      if (aSolr != null) {
        aSolr.tearDown();
      }
    }
    super.tearDown();
  }

  private LBClientHolder client(LBSolrClient.Endpoint... baseSolrEndpoints) {
    if (random().nextBoolean()) {
      var delegateClient =
          new HttpJettySolrClient.Builder()
              .withConnectionTimeout(1000, TimeUnit.MILLISECONDS)
              .withIdleTimeout(2000, TimeUnit.MILLISECONDS)
              .build();
      var lbClient =
          new LBSolrClient.Builder<>(delegateClient, baseSolrEndpoints)
              .withDefaultCollection(solr[0].getDefaultCollection())
              .setAliveCheckInterval(500, TimeUnit.MILLISECONDS)
              .build();
      return new LBClientHolder(lbClient, delegateClient);
    } else {
      var delegateClient =
          new HttpJdkSolrClient.Builder()
              .withConnectionTimeout(1000, TimeUnit.MILLISECONDS)
              .withIdleTimeout(2000, TimeUnit.MILLISECONDS)
              .withSSLContext(MockTrustManager.ALL_TRUSTING_SSL_CONTEXT)
              .build();
      var lbClient =
          new LBSolrClient.Builder<>(delegateClient, baseSolrEndpoints)
              .withDefaultCollection(solr[0].getDefaultCollection())
              .setAliveCheckInterval(500, TimeUnit.MILLISECONDS)
              .build();
      return new LBClientHolder(lbClient, delegateClient);
    }
  }

  public void testSimple() throws Exception {
    final var baseSolrEndpoints = bootstrapBaseSolrEndpoints(solr.length);
    try (var h = client(baseSolrEndpoints)) {
      SolrQuery solrQuery = new SolrQuery("*:*");
      Set<String> names = new HashSet<>();
      QueryResponse resp = null;
      for (int i = 0; i < solr.length; i++) {
        resp = h.lbClient.query(solrQuery);
        assertEquals(10, resp.getResults().getNumFound());
        names.add(resp.getResults().get(0).getFieldValue("name").toString());
      }
      assertEquals(3, names.size());

      // Kill a server and test again
      solr[1].jetty.stop();
      solr[1].jetty = null;
      names.clear();
      for (int i = 0; i < solr.length; i++) {
        resp = h.lbClient.query(solrQuery);
        assertEquals(10, resp.getResults().getNumFound());
        names.add(resp.getResults().get(0).getFieldValue("name").toString());
      }
      assertEquals(2, names.size());
      assertFalse(names.contains("solr1"));

      startJettyAndWaitForAliveCheckQuery(solr[1]);

      names.clear();
      for (int i = 0; i < solr.length; i++) {
        resp = h.lbClient.query(solrQuery);
        assertEquals(10, resp.getResults().getNumFound());
        names.add(resp.getResults().get(0).getFieldValue("name").toString());
      }
      assertEquals(3, names.size());
    }
  }

  public void testTwoServers() throws Exception {
    final var baseSolrEndpoints = bootstrapBaseSolrEndpoints(2);
    try (var h = client(baseSolrEndpoints)) {
      SolrQuery solrQuery = new SolrQuery("*:*");
      QueryResponse resp = null;
      solr[0].jetty.stop();
      solr[0].jetty = null;
      resp = h.lbClient.query(solrQuery);
      String name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection11", name);
      resp = h.lbClient.query(solrQuery);
      name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection11", name);

      solr[1].jetty.stop();
      solr[1].jetty = null;
      startJettyAndWaitForAliveCheckQuery(solr[0]);

      RetryUtil.retryOnBoolean(
          5000,
          500,
          () -> {
            try {
              return ("solr/collection10"
                  .equals(
                      h.lbClient
                          .query(solrQuery)
                          .getResults()
                          .get(0)
                          .getFieldValue("name")
                          .toString()));
            } catch (Exception e) {
              return false;
            }
          });
    }
  }

  public void testTimeoutExceptionMarksServerAsZombie() throws Exception {
    try (TimeoutZombieTestContext ctx = new TimeoutZombieTestContext()) {
      LBSolrClient.Req lbReq = ctx.createQueryRequest();

      try {
        ctx.lbClient.request(lbReq);
      } catch (Exception e) {
      }

      ctx.assertZombieState();
    }
  }

  public void testTimeoutExceptionMarksServerAsZombieAsyncRequest() throws Exception {
    try (TimeoutZombieTestContext ctx = new TimeoutZombieTestContext()) {
      LBSolrClient.Req lbReq = ctx.createQueryRequest();

      ctx.lbClient.requestAsync(lbReq).exceptionally(e -> null).get();

      ctx.assertZombieState();
    }
  }

  public void testConnectTimeoutExceptionMarksServerAsZombie() throws Exception {
    try (ConnectTimeoutZombieTestContext ctx = new ConnectTimeoutZombieTestContext()) {
      LBSolrClient.Req lbReq = ctx.createQueryRequest();

      try {
        ctx.lbClient.request(lbReq);
      } catch (Exception e) {
      }

      ctx.assertZombieState();
    }
  }

  public void testConnectTimeoutExceptionMarksServerAsZombieAsyncRequest() throws Exception {
    try (ConnectTimeoutZombieTestContext ctx = new ConnectTimeoutZombieTestContext()) {
      LBSolrClient.Req lbReq = ctx.createQueryRequest();

      ctx.lbClient.requestAsync(lbReq).exceptionally(e -> null).get();

      ctx.assertZombieState();
    }
  }

  public void testConnectTimeoutExceptionMarksServerAsZombieAsyncUpdate() throws Exception {
    try (ConnectTimeoutZombieTestContext ctx = new ConnectTimeoutZombieTestContext()) {
      LBSolrClient.Req lbReq = ctx.createUpdateRequest();

      ctx.lbClient.requestAsync(lbReq).exceptionally(e -> null).get();

      ctx.assertZombieState();
    }
  }

  private LBSolrClient.Endpoint[] bootstrapBaseSolrEndpoints(int max) {
    LBSolrClient.Endpoint[] solrUrls = new LBSolrClient.Endpoint[max];
    for (int i = 0; i < max; i++) {
      solrUrls[i] = new LBSolrClient.Endpoint(solr[i].getBaseUrl());
    }
    return solrUrls;
  }

  private void startJettyAndWaitForAliveCheckQuery(SolrInstance solrInstance) throws Exception {
    try (LogListener logListener =
        LogListener.debug(LBSolrClient.class).substring(LBSolrClient.UPDATE_LIVE_SERVER_MESSAGE)) {
      solrInstance.startJetty();
      if (logListener.pollMessage(10, TimeUnit.SECONDS) == null) {
        fail("The alive check query was not logged within 10 seconds.");
      }
    }
  }

  private static class SolrInstance {
    String name;
    Path homeDir;
    Path dataDir;
    Path confDir;
    int port;
    JettySolrRunner jetty;

    public SolrInstance(String name, Path homeDir, int port) {
      this.name = name;
      this.homeDir = homeDir;
      this.port = port;

      dataDir = homeDir.resolve("collection1").resolve("data");
      confDir = homeDir.resolve("collection1").resolve("conf");
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getBaseUrl() {
      return buildUrl(port);
    }

    public String getDefaultCollection() {
      return "collection1";
    }

    public String getSchemaFile() {
      return "solrj/solr/collection1/conf/schema-replication1.xml";
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      return "solrj/solr/collection1/conf/solrconfig-follower1.xml";
    }

    public void setUp() throws Exception {
      Files.createDirectories(homeDir);
      Files.createDirectories(dataDir);
      Files.createDirectories(confDir);

      Path f = confDir.resolve("solrconfig.xml");
      Files.copy(SolrTestCaseJ4.getFile(getSolrConfigFile()), f);
      f = confDir.resolve("schema.xml");
      Files.copy(SolrTestCaseJ4.getFile(getSchemaFile()), f);
      Files.createFile(homeDir.resolve("collection1/core.properties"));
    }

    public void tearDown() throws Exception {
      if (jetty != null) jetty.stop();
      IOUtils.rm(homeDir);
    }

    public void startJetty() throws Exception {

      Properties props = new Properties();
      props.setProperty("solrconfig", "bad_solrconfig.xml");
      props.setProperty("solr.data.dir", getDataDir());

      JettyConfig jettyConfig = JettyConfig.builder().setPort(port).build();

      jetty = new JettySolrRunner(getHomeDir(), props, jettyConfig);
      jetty.start();
      int newPort = jetty.getLocalPort();
      if (port != 0 && newPort != port) {
        fail("TESTING FAILURE: could not grab requested port.");
      }
      this.port = newPort;
    }
  }

  private static class LBClientHolder implements AutoCloseable {

    final LBSolrClient lbClient;
    final SolrClient delegate; // http

    LBClientHolder(LBSolrClient lbClient, SolrClient delegate) {
      this.lbClient = lbClient;
      this.delegate = delegate;
    }

    @Override
    public void close() {
      lbClient.close();
      try {
        delegate.close();
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }
  }

  private class TimeoutZombieTestContext implements AutoCloseable {
    final ServerSocket blackhole;
    final LBSolrClient.Endpoint nonRoutableEndpoint;
    final HttpJettySolrClient delegateClient;
    final LBAsyncSolrClient lbClient;

    TimeoutZombieTestContext() throws Exception {
      // create a socket that allows a client to connect but causes them to hang until idleTimeout
      // is triggered
      blackhole = new ServerSocket(0);
      int blackholePort = blackhole.getLocalPort();
      nonRoutableEndpoint =
          new LBSolrClient.Endpoint("http://localhost:" + blackholePort + "/solr");

      delegateClient =
          new HttpJettySolrClient.Builder()
              .withConnectionTimeout(1000, TimeUnit.MILLISECONDS)
              .withIdleTimeout(1, TimeUnit.MILLISECONDS)
              .build();

      lbClient = new LBJettySolrClient.Builder(delegateClient, nonRoutableEndpoint).build();
    }

    LBSolrClient.Req createQueryRequest() {
      SolrQuery solrQuery = new SolrQuery("*:*");
      QueryRequest queryRequest = new QueryRequest(solrQuery);

      List<LBSolrClient.Endpoint> endpoints =
          List.of(
              new LBSolrClient.Endpoint(
                  nonRoutableEndpoint.getBaseUrl(), solr[0].getDefaultCollection()));
      return new LBSolrClient.Req(queryRequest, endpoints);
    }

    void assertZombieState() {
      assertTrue(
          "Non-routable endpoint should be marked as zombie due to timeout",
          lbClient.zombieServers.containsKey(
              nonRoutableEndpoint.getBaseUrl() + "/" + solr[0].getDefaultCollection()));
    }

    @Override
    public void close() {
      lbClient.close();
      delegateClient.close();
      try {
        blackhole.close();
      } catch (IOException ioe) {

      }
    }
  }

  private class ConnectTimeoutZombieTestContext implements AutoCloseable {
    final ServerSocket ss;
    final Socket connector;
    final LBSolrClient.Endpoint nonRoutableEndpoint;
    final LBAsyncSolrClient lbClient;
    final HttpJdkSolrClient delegateClient;

    ConnectTimeoutZombieTestContext() throws Exception {
      // Create a server socket with a backlog of 1 and occupy that slot to trigger a connect
      // timeout.
      ss = new ServerSocket(0, 1);
      int port = ss.getLocalPort();
      connector = new Socket("127.0.0.1", port);

      nonRoutableEndpoint = new LBSolrClient.Endpoint("http://127.0.0.1:" + port + "/solr");
      delegateClient =
          new HttpJdkSolrClient.Builder(nonRoutableEndpoint.getBaseUrl())
              .withConnectionTimeout(1, TimeUnit.MILLISECONDS)
              .build();

      lbClient =
          new LBAsyncSolrClient(
              new LBSolrClient.Builder<>(delegateClient, nonRoutableEndpoint)
                  .withDefaultCollection(solr[0].getDefaultCollection())) {
            @Override
            protected CompletableFuture<NamedList<Object>> requestAsyncWithUrl(
                SolrClient client, String baseUrl, SolrRequest<?> request)
                throws SolrServerException, IOException {
              return ((HttpJdkSolrClient) client).requestAsync(request, null);
            }
          };
    }

    LBSolrClient.Req createQueryRequest() {
      SolrQuery solrQuery = new SolrQuery("*:*");
      QueryRequest queryRequest = new QueryRequest(solrQuery);

      List<LBSolrClient.Endpoint> endpoints =
          List.of(
              new LBSolrClient.Endpoint(
                  nonRoutableEndpoint.getBaseUrl(), solr[0].getDefaultCollection()));
      return new LBSolrClient.Req(queryRequest, endpoints);
    }

    LBSolrClient.Req createUpdateRequest() {
      UpdateRequest updateRequest = new UpdateRequest();
      updateRequest.add(new SolrInputDocument());

      List<LBSolrClient.Endpoint> endpoints =
          List.of(
              new LBSolrClient.Endpoint(
                  nonRoutableEndpoint.getBaseUrl(), solr[0].getDefaultCollection()));
      return new LBSolrClient.Req(updateRequest, endpoints);
    }

    void assertZombieState() {
      assertTrue(
          "Endpoint should be marked as zombie due to connect timeout",
          lbClient.zombieServers.containsKey(
              nonRoutableEndpoint.getBaseUrl() + "/" + solr[0].getDefaultCollection()));
    }

    @Override
    public void close() throws IOException {
      lbClient.close();
      delegateClient.close();
      connector.close();
      ss.close();
    }
  }
}
