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

import static org.apache.solr.SolrTestCaseJ4.DEFAULT_TEST_CORENAME;
import static org.apache.solr.SolrTestCaseJ4.getHttpSolrClient;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrJettyTestRule extends SolrClientTestRule {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private SolrClient client = null;
  private JettySolrRunner jetty;
  private SolrClient adminClient = null;

  private ConcurrentHashMap<String, SolrClient> clients =
      new ConcurrentHashMap<>(); // TODO close them

  public SolrClient getClient() {
    return client;
  }

  @Override
  protected void after() {
    if (adminClient != null) {
      try {
        adminClient.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
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

    if (client != null) {
      try {
        client.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      client = null;
    }
  }

  @Deprecated // Prefer not to have this.
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
    if (clients.containsKey(name)) {
      return clients.get(name);
    } else {
      clients.put(
          name, getHttpSolrClient(jetty.getBaseUrl().toString() + "/" + DEFAULT_TEST_CORENAME));
      return getHttpSolrClient(jetty.getBaseUrl().toString() + "/" + DEFAULT_TEST_CORENAME);
    }
  }

  @Override
  public SolrClient getAdminClient() {
    return adminClient;
  }
}
