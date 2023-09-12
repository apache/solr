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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
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

  private JettySolrRunner jetty;

  private final ConcurrentHashMap<String, SolrClient> clients = new ConcurrentHashMap<>();
  private boolean enableProxy;

  @Override
  protected void after() {
    for (SolrClient solrClient : clients.values()) {
      IOUtils.closeQuietly(solrClient);
    }
    clients.clear();

    if (jetty != null) {
      try {
        jetty.stop();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      jetty = null;
      enableProxy = false;
    }
  }

  /** Resets the state. DEPRECATED; please don't call! */
  @Deprecated
  public void reset() {
    after();
  }

  @Override
  public void startSolr(Path solrHome) {
    startSolr(
        solrHome,
        new Properties(),
        JettyConfig.builder()
            .withSSLConfig(SolrTestCaseJ4.sslConfig.buildServerSSLConfig())
            .build());
  }

  /**
   * Enables proxy feature to allow for failure injection testing at the inter-node communication
   * level. Must be called prior to starting.
   *
   * @see JettySolrRunner#getProxy()
   */
  public void enableProxy() {
    assert jetty == null;
    this.enableProxy = true;
  }

  public void startSolr(Path solrHome, Properties nodeProperties, JettyConfig jettyConfig) {
    if (jetty != null) throw new IllegalStateException("Jetty is already running");

    jetty = new JettySolrRunner(solrHome.toString(), nodeProperties, jettyConfig, enableProxy);
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
    if (jetty == null) throw new IllegalStateException("Jetty has not started");
    return jetty;
  }

  @Override
  public SolrClient getSolrClient(String collection) {
    if (collection == null) {
      collection = "";
    }
    return clients.computeIfAbsent(collection, this::newSolrClient);
  }

  protected SolrClient newSolrClient(String collection) {
    String url = getBaseUrl() + (StrUtils.isBlank(collection) ? "" : "/" + collection);
    return new HttpSolrClient.Builder(url).build();
  }

  /** URL to Solr. */
  public String getBaseUrl() {
    return getJetty().getBaseUrl().toString();
  }

  public CoreContainer getCoreContainer() {
    return getJetty().getCoreContainer();
  }
}
