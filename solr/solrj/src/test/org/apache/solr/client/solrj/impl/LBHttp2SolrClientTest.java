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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for LBHttp2SolrClient
 *
 * @since solr 1.4
 */
public class LBHttp2SolrClientTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  SolrInstance[] solr = new SolrInstance[3];
  Http2SolrClient httpClient;

  // TODO: fix this test to not require FSDirectory
  static String savedFactory;

  @BeforeClass
  public static void beforeClass() {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
  }

  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
    System.clearProperty("tests.shardhandler.randomSeed");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    httpClient =
        new Http2SolrClient.Builder()
            .withConnectionTimeout(1000, TimeUnit.MILLISECONDS)
            .withIdleTimeout(2000, TimeUnit.MILLISECONDS)
            .build();

    for (int i = 0; i < solr.length; i++) {
      solr[i] =
          new SolrInstance("solr/collection1" + i, createTempDir("instance-" + i).toFile(), 0);
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
    httpClient.close();
    super.tearDown();
  }

  public void testSimple() throws Exception {
    final var baseSolrEndpoints = bootstrapBaseSolrEndpoints(solr.length);
    try (LBHttp2SolrClient client =
        new LBHttp2SolrClient.Builder(httpClient, baseSolrEndpoints)
            .withDefaultCollection(solr[0].getDefaultCollection())
            .setAliveCheckInterval(500, TimeUnit.MILLISECONDS)
            .build()) {
      SolrQuery solrQuery = new SolrQuery("*:*");
      Set<String> names = new HashSet<>();
      QueryResponse resp = null;
      for (int i = 0; i < solr.length; i++) {
        resp = client.query(solrQuery);
        assertEquals(10, resp.getResults().getNumFound());
        names.add(resp.getResults().get(0).getFieldValue("name").toString());
      }
      assertEquals(3, names.size());

      // Kill a server and test again
      solr[1].jetty.stop();
      solr[1].jetty = null;
      names.clear();
      for (int i = 0; i < solr.length; i++) {
        resp = client.query(solrQuery);
        assertEquals(10, resp.getResults().getNumFound());
        names.add(resp.getResults().get(0).getFieldValue("name").toString());
      }
      assertEquals(2, names.size());
      assertFalse(names.contains("solr1"));

      // Start the killed server once again
      solr[1].startJetty();
      // Wait for the alive check to complete
      Thread.sleep(1200);
      names.clear();
      for (int i = 0; i < solr.length; i++) {
        resp = client.query(solrQuery);
        assertEquals(10, resp.getResults().getNumFound());
        names.add(resp.getResults().get(0).getFieldValue("name").toString());
      }
      assertEquals(3, names.size());
    }
  }

  public void testTwoServers() throws Exception {
    final var baseSolrEndpoints = bootstrapBaseSolrEndpoints(2);
    try (LBHttp2SolrClient client =
        new LBHttp2SolrClient.Builder(httpClient, baseSolrEndpoints)
            .withDefaultCollection(solr[0].getDefaultCollection())
            .setAliveCheckInterval(500, TimeUnit.MILLISECONDS)
            .build()) {
      SolrQuery solrQuery = new SolrQuery("*:*");
      QueryResponse resp = null;
      solr[0].jetty.stop();
      solr[0].jetty = null;
      resp = client.query(solrQuery);
      String name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection11", name);
      resp = client.query(solrQuery);
      name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection11", name);
      solr[1].jetty.stop();
      solr[1].jetty = null;
      solr[0].startJetty();
      Thread.sleep(1200);
      try {
        resp = client.query(solrQuery);
      } catch (SolrServerException e) {
        // try again after a pause in case the error is lack of time to start server
        Thread.sleep(3000);
        resp = client.query(solrQuery);
      }
      name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection10", name);
    }
  }

  public void testReliability() throws Exception {
    final var baseSolrEndpoints = bootstrapBaseSolrEndpoints(solr.length);

    try (LBHttp2SolrClient client =
        new LBHttp2SolrClient.Builder(httpClient, baseSolrEndpoints)
            .withDefaultCollection(solr[0].getDefaultCollection())
            .setAliveCheckInterval(500, TimeUnit.MILLISECONDS)
            .build()) {

      // Kill a server and test again
      solr[1].jetty.stop();
      solr[1].jetty = null;

      // query the servers
      for (int i = 0; i < solr.length; i++) client.query(new SolrQuery("*:*"));

      // Start the killed server once again
      solr[1].startJetty();
      // Wait for the alive check to complete
      waitForServer(30, client, 3, solr[1].name);
    }
  }

  /**
   * Test method for {@link LBHttp2SolrClient.Builder} that validates that the query param keys
   * passed in by the base <code>Http2SolrClient
   * </code> instance are used by the LBHttp2SolrClient.
   */
  @Test
  public void testLBHttp2SolrClientWithTheseParamNamesInTheUrl() {
    String url = "http://127.0.0.1:8080";
    Set<String> urlParamNames = new HashSet<>(2);
    urlParamNames.add("param1");

    try (Http2SolrClient http2SolrClient =
                 new Http2SolrClient.Builder(url).withTheseParamNamesInTheUrl(urlParamNames).build();
         LBHttp2SolrClient testClient =
                 new LBHttp2SolrClient.Builder(http2SolrClient, new LBSolrClient.Endpoint(url))
                         .build()) {

      assertArrayEquals(
              "Wrong urlParamNames found in lb client.",
              urlParamNames.toArray(),
              testClient.getUrlParamNames().toArray());
      assertArrayEquals(
              "Wrong urlParamNames found in base client.",
              urlParamNames.toArray(),
              http2SolrClient.getUrlParamNames().toArray());
    }
  }

  @Test
  public void testAsyncWithFailures() {

    // This demonstrates that the failing endpoint always gets retried, and it is up to the user
    // to remove any failing nodes if desired.

    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    Http2SolrClient.Builder b =
            new Http2SolrClient.Builder("http://base.url").withConnectionTimeout(10, TimeUnit.SECONDS);
    ;
    try (MockHttp2SolrClient client = new MockHttp2SolrClient("http://base.url", b);
         LBHttp2SolrClient testClient = new LBHttp2SolrClient.Builder(client, ep1, ep2).build()) {

      for (int j = 0; j < 2; j++) {
        // first time Endpoint One will return error code 500.
        // second time Endpoint One will be healthy

        String basePathToSucceed;
        if (j == 0) {
          client.basePathToFail = ep1.getBaseUrl();
          basePathToSucceed = ep2.getBaseUrl();
        } else {
          client.basePathToFail = ep2.getBaseUrl();
          basePathToSucceed = ep1.getBaseUrl();
        }

        for (int i = 0; i < 10; i++) {
          // i: we'll try 10 times to see if it behaves the same every time.

          QueryRequest queryRequest = new QueryRequest(new MapSolrParams(Map.of("q", "" + i)));
          LBSolrClient.Req req = new LBSolrClient.Req(queryRequest, endpointList);
          String iterMessage = "iter j/i " + j + "/" + i;
          try {
            testClient.requestAsync(req).get(1, TimeUnit.MINUTES);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            fail("interrupted");
          } catch (TimeoutException | ExecutionException e) {
            fail(iterMessage + " Response ended in failure: " + e);
          }
          if (i == 0) {
            // When j=0, "endpoint one" fails.
            // The first time around (i) it tries the first, then the second.
            //
            // With j=0 and i>0, it only tries "endpoint two".
            //
            // When j=1 and i=0, "endpoint two" starts failing.
            // So it tries both it and "endpoint one"
            //
            // With j=1 and i>0, it only tries "endpoint one".
            assertEquals(iterMessage, 2, client.lastBasePaths.size());

            String failedBasePath = client.lastBasePaths.remove(0);
            assertEquals(iterMessage, client.basePathToFail, failedBasePath);
          } else {
            // The first endpoint does not give the exception, it doesn't retry.
            assertEquals(iterMessage, 1, client.lastBasePaths.size());
          }
          String successBasePath = client.lastBasePaths.remove(0);
          assertEquals(iterMessage, basePathToSucceed, successBasePath);
        }
      }
    }
  }

  @Test
  public void testAsync() {
    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    Http2SolrClient.Builder b =
            new Http2SolrClient.Builder("http://base.url").withConnectionTimeout(10, TimeUnit.SECONDS);
    try (MockHttp2SolrClient client = new MockHttp2SolrClient("http://base.url", b);
         LBHttp2SolrClient testClient = new LBHttp2SolrClient.Builder(client, ep1, ep2).build()) {

      int limit = 10; // For simplicity use an even limit
      List<CompletableFuture<LBSolrClient.Rsp>> responses = new ArrayList<>();

      for (int i = 0; i < limit; i++) {
        QueryRequest queryRequest = new QueryRequest(new MapSolrParams(Map.of("q", "" + i)));
        LBSolrClient.Req req = new LBSolrClient.Req(queryRequest, endpointList);
        responses.add(testClient.requestAsync(req));
      }

      QueryRequest[] queryRequests = new QueryRequest[limit];
      int numEndpointOne = 0;
      int numEndpointTwo = 0;
      for (int i = 0; i < limit; i++) {
        SolrRequest<?> lastSolrReq = client.lastSolrRequests.get(i);
        assertTrue(lastSolrReq instanceof QueryRequest);
        QueryRequest lastQueryReq = (QueryRequest) lastSolrReq;
        int index = Integer.parseInt(lastQueryReq.getParams().get("q"));
        assertNull("Found same request twice: " + index, queryRequests[index]);
        queryRequests[index] = lastQueryReq;
        if (lastQueryReq.getBasePath().equals(ep1.toString())) {
          numEndpointOne++;
        } else if (lastQueryReq.getBasePath().equals(ep2.toString())) {
          numEndpointTwo++;
        }

        LBSolrClient.Rsp lastRsp = null;
        try {
          lastRsp = responses.get(index).get();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          fail("interrupted");
        } catch (ExecutionException ee) {
          fail("Response " + index + " ended in failure: " + ee);
        }
        NamedList<Object> lastResponse = lastRsp.getResponse();

        // The Mock will return {"response": index}.
        assertEquals("" + index, lastResponse.get("response"));
      }

      // It is the user's responsibility to shuffle the endpoints when using
      // async.  LB Http Solr Client will always try the passed-in endpoints
      // in order.  In this case, endpoint 1 gets all the requests!
      assertEquals(limit, numEndpointOne);
      assertEquals(0, numEndpointTwo);

      assertEquals(limit, client.lastSolrRequests.size());
      assertEquals(limit, client.lastCollections.size());
    }
  }

  // wait maximum ms for serverName to come back up
  private void waitForServer(
          int maxSeconds, LBHttp2SolrClient client, int nServers, String serverName) throws Exception {
    final TimeOut timeout = new TimeOut(maxSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      QueryResponse resp;
      try {
        resp = client.query(new SolrQuery("*:*"));
      } catch (Exception e) {
        log.warn("", e);
        continue;
      }
      String name = resp.getResults().get(0).getFieldValue("name").toString();
      if (name.equals(serverName)) return;

      Thread.sleep(500);
    }
  }

  private LBSolrClient.Endpoint[] bootstrapBaseSolrEndpoints(int max) {
    LBSolrClient.Endpoint[] solrUrls = new LBSolrClient.Endpoint[max];
    for (int i = 0; i < max; i++) {
      solrUrls[i] = new LBSolrClient.Endpoint(solr[i].getBaseUrl());
    }
    return solrUrls;
  }

  private static class SolrInstance {
    String name;
    File homeDir;
    File dataDir;
    File confDir;
    int port;
    JettySolrRunner jetty;

    public SolrInstance(String name, File homeDir, int port) {
      this.name = name;
      this.homeDir = homeDir;
      this.port = port;

      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getUrl() {
      return buildUrl(port) + "/collection1";
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

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      return "solrj/solr/collection1/conf/solrconfig-follower1.xml";
    }

    public String getSolrXmlFile() {
      return "solrj/solr/solr.xml";
    }

    public void setUp() throws Exception {
      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      Files.copy(
          SolrTestCaseJ4.getFile(getSolrXmlFile()).toPath(), homeDir.toPath().resolve("solr.xml"));

      Path f = confDir.toPath().resolve("solrconfig.xml");
      Files.copy(SolrTestCaseJ4.getFile(getSolrConfigFile()).toPath(), f);
      f = confDir.toPath().resolve("schema.xml");
      Files.copy(SolrTestCaseJ4.getFile(getSchemaFile()).toPath(), f);
      Files.createFile(homeDir.toPath().resolve("collection1/core.properties"));
    }

    public void tearDown() throws Exception {
      if (jetty != null) jetty.stop();
      IOUtils.rm(homeDir.toPath());
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
      //      System.out.println("waiting.........");
      //      Thread.sleep(5000);
    }
  }

  public static class MockHttp2SolrClient extends Http2SolrClient {

    public List<SolrRequest<?>> lastSolrRequests = new ArrayList<>();

    public List<String> lastBasePaths = new ArrayList<>();

    public List<String> lastCollections = new ArrayList<>();

    public String basePathToFail = null;

    protected MockHttp2SolrClient(String serverBaseUrl, Builder builder) {
      // TODO: Consider creating an interface for Http*SolrClient
      // so mocks can Implement, not Extend, and not actually need to
      // build an (unused) client
      super(serverBaseUrl, builder);
    }

    @Override
    public CompletableFuture<NamedList<Object>> requestAsync(
            final SolrRequest<?> solrRequest, String collection) {
      CompletableFuture<NamedList<Object>> cf = new CompletableFuture<>();
      lastSolrRequests.add(solrRequest);
      lastBasePaths.add(solrRequest.getBasePath());
      lastCollections.add(collection);
      if (solrRequest.getBasePath().equals(basePathToFail)) {
        cf.completeExceptionally(
                new SolrException(SolrException.ErrorCode.SERVER_ERROR, "We should retry this."));
      } else {
        cf.complete(generateResponse(solrRequest));
      }
      return cf;
    }

    private NamedList<Object> generateResponse(SolrRequest<?> solrRequest) {
      String id = solrRequest.getParams().get("q");
      return new NamedList<>(Collections.singletonMap("response", id));
    }
  }

}
