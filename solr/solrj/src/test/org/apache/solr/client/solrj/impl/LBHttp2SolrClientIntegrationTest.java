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
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.LogListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for LBHttp2SolrClient
 *
 * @since solr 1.4
 */
@LogLevel("org.apache.solr.client.solrj.impl=DEBUG")
public class LBHttp2SolrClientIntegrationTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  SolrInstance[] solr = new SolrInstance[3];

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
        try {
          aSolr.tearDown();
        } catch (Exception e) {
          // Log but don't fail the test due to teardown issues
          log.warn("Exception during tearDown of {}", aSolr.name, e);
        }
      }
    }
    super.tearDown();
  }

  private LBClientHolder client(LBSolrClient.Endpoint... baseSolrEndpoints) {
    if (random().nextBoolean()) {
      var delegateClient =
          new Http2SolrClient.Builder()
              .withConnectionTimeout(1000, TimeUnit.MILLISECONDS)
              .withIdleTimeout(2000, TimeUnit.MILLISECONDS)
              .build();
      var lbClient =
          new LBHttp2SolrClient.Builder<>(delegateClient, baseSolrEndpoints)
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
          new LBHttp2SolrClient.Builder<>(delegateClient, baseSolrEndpoints)
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
      try {
        solr[1].jetty.stop();
      } catch (Exception e) {
        log.warn("Exception stopping jetty during test", e);
      } finally {
        solr[1].jetty = null;
      }

      // Give some time for the LB client to detect the server is down
      Thread.sleep(100);

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

      try {
        solr[0].jetty.stop();
      } catch (Exception e) {
        log.warn("Exception stopping jetty[0] during test", e);
      } finally {
        solr[0].jetty = null;
      }

      Thread.sleep(100); // Give LB client time to detect server down

      resp = h.lbClient.query(solrQuery);
      String name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection11", name);
      resp = h.lbClient.query(solrQuery);
      name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection11", name);

      try {
        solr[1].jetty.stop();
      } catch (Exception e) {
        log.warn("Exception stopping jetty[1] during test", e);
      } finally {
        solr[1].jetty = null;
      }

      startJettyAndWaitForAliveCheckQuery(solr[0]);

      resp = h.lbClient.query(solrQuery);
      name = resp.getResults().get(0).getFieldValue("name").toString();
      assertEquals("solr/collection10", name);
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

  private static void stopJettyWithTimeout(JettySolrRunner jetty, long timeoutMs) throws Exception {
    final var executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("jetty-shutdown"));

    try {
      final var future =
          java.util.concurrent.CompletableFuture.runAsync(
              () -> {
                try {
                  jetty.stop();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              },
              executor);

      try {
        future.get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
        future.cancel(true);
        throw new Exception("Jetty shutdown timed out after " + timeoutMs + "ms", e);
      }
    } finally {
      executor.shutdown();
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
      Files.createDirectories(homeDir);
      Files.createDirectories(dataDir);
      Files.createDirectories(confDir);

      Files.copy(SolrTestCaseJ4.getFile(getSolrXmlFile()), homeDir.resolve("solr.xml"));

      Path f = confDir.resolve("solrconfig.xml");
      Files.copy(SolrTestCaseJ4.getFile(getSolrConfigFile()), f);
      f = confDir.resolve("schema.xml");
      Files.copy(SolrTestCaseJ4.getFile(getSchemaFile()), f);
      Files.createFile(homeDir.resolve("collection1/core.properties"));
    }

    public void tearDown() throws Exception {
      if (jetty != null) {
        try {
          // Use a timeout wrapper for shutdown
          stopJettyWithTimeout(jetty, 5000);
        } catch (Exception e) {
          log.warn("Exception stopping jetty for {}", name, e);
        } finally {
          jetty = null;
        }
      }
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

    final LBHttp2SolrClient<?> lbClient;
    final HttpSolrClientBase delegate;

    LBClientHolder(LBHttp2SolrClient<?> lbClient, HttpSolrClientBase delegate) {
      this.lbClient = lbClient;
      this.delegate = delegate;
    }

    @Override
    public void close() {
      try {
        lbClient.close();
      } catch (Exception e) {
        log.warn("Exception closing LB client", e);
      }
      try {
        delegate.close();
      } catch (IOException ioe) {
        log.warn("Exception closing delegate client", ioe);
        // Don't throw exception during cleanup to avoid masking test failures
      }
    }
  }
}
