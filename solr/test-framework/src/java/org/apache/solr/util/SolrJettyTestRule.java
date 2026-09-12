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
package org.apache.solr.util;

import static org.apache.solr.SolrTestCaseJ4.DEFAULT_TEST_COLLECTION_NAME;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SolrClientTestRule} that provides a Solr instance running in Jetty, an HTTP server. It's
 * based off of {@link JettySolrRunner}.
 */
public class SolrJettyTestRule extends SolrClientTestRule {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ConcurrentHashMap<String, HttpSolrClient> clients = new ConcurrentHashMap<>();
  private boolean enableProxy;

  @Override
  protected void after() {
    for (var solrClient : clients.values()) {
      IOUtils.closeQuietly(solrClient);
    }
    clients.clear();
    enableProxy = false;

    super.after(); // closes the backend (JettySolrRunner)
  }

  /**
   * Resets the state.
   *
   * @deprecated Please don't call! There is no replacement API.
   */
  @Deprecated(since = "10.0")
  public void reset() {
    after();
  }

  @Override
  public void startSolr(Path solrHome) {
    startSolr(solrHome, new Properties(), JettyConfig.builder().build());
  }

  /**
   * Enables proxy feature to allow for failure injection testing at the inter-node communication
   * level. Must be called prior to starting.
   *
   * @see JettySolrRunner#getProxy()
   */
  public void enableProxy() {
    assert backend == null;
    this.enableProxy = true;
  }

  public void startSolr(Path solrHome, Properties nodeProperties, JettyConfig jettyConfig) {
    if (backend != null) throw new IllegalStateException("Jetty is already running");

    var jetty = new JettySolrRunner(solrHome.toString(), nodeProperties, jettyConfig, enableProxy);
    backend = jetty;
    try {
      jetty.start();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    int port = jetty.getLocalPort();
    log.info("Jetty Assigned Port#{}", port);
  }

  public JettySolrRunner getJetty() {
    if (backend == null) throw new IllegalStateException("Jetty has not started");
    return (JettySolrRunner) backend;
  }

  @Override
  public HttpSolrClient getSolrClient(String collection) {
    if (collection == null) {
      collection = "";
    }
    return clients.computeIfAbsent(collection, this::newSolrClient);
  }

  @Override
  public HttpSolrClient getAdminClient() {
    // Use an HTTP client so requests route through Jetty, not the embedded shortcut.
    return getSolrClient(null);
  }

  protected HttpSolrClient newSolrClient(String collection) {
    return newSolrClientBuilder()
        .withDefaultCollection(collection) // Properly handles when collection is 'null'
        .build();
  }

  /**
   * Creates a client builder with the URL, shared Jetty HttpClient, and default collection
   * "collection1" (can be changed).
   */
  public HttpJettySolrClient.Builder newSolrClientBuilder() {
    return new HttpJettySolrClient.Builder(getBaseUrl())
        .withHttpClient((HttpJettySolrClient) backend.getSolrClient())
        .withDefaultCollection(DEFAULT_TEST_COLLECTION_NAME);
  }

  /** URL to Solr. */
  public String getBaseUrl() {
    return getJetty().getBaseUrl().toString();
  }

  @Override
  protected void createColl(NewCollectionBuilder b) throws SolrServerException, IOException {
    createCollStandalone(b);
  }
}
