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

import static org.apache.solr.SolrTestCaseJ4.getHttpSolrClient;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides a JettySolrRunner for tests. It starts and stops Solr Server instance based on jetty */
public class SolrJettyTestRule extends SolrClientTestRule {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private JettySolrRunner jetty;

  private SolrClient adminClient = null;
  private final ConcurrentHashMap<String, SolrClient> clients = new ConcurrentHashMap<>();

  @Override
  protected void after() {
    for (SolrClient solrClient : clients.values()) {
      IOUtils.closeQuietly(solrClient);
    }
    clients.clear();

    IOUtils.closeQuietly(adminClient);
    adminClient = null;

    if (jetty != null) {
      try {
        jetty.stop();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      jetty = null;
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

  public void startSolr(Path solrHome, Properties nodeProperties, JettyConfig jettyConfig) {
    if (jetty != null) throw new IllegalStateException("Jetty is already running");

    jetty = new JettySolrRunner(solrHome.toString(), nodeProperties, jettyConfig);
    try {
      jetty.start();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    int port = jetty.getLocalPort();
    log.info("Jetty Assigned Port#{}", port);
    adminClient = getHttpSolrClient(jetty.getBaseUrl().toString());
  }

  public JettySolrRunner getJetty() {
    if (jetty == null) throw new IllegalStateException("Jetty has not started");
    return jetty;
  }

  @Override
  public SolrClient getSolrClient(String name) {
    return clients.computeIfAbsent(
        name, key -> new HttpSolrClient.Builder(getBaseUrl() + "/" + name).build());
  }

  /** URL to Solr. */
  public String getBaseUrl() {
    return getJetty().getBaseUrl().toString();
  }

  @Override
  public SolrClient getAdminClient() {
    return adminClient;
  }
}
