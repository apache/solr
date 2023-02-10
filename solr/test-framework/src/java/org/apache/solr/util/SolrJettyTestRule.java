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
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrJettyTestRule extends SolrClientTestRule {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private JettySolrRunner jetty;
  private SolrClient adminClient = null;

  @Override
  protected void after() {
    if (adminClient != null) {
      try {
        adminClient.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    adminClient = null;

    if (jetty != null) {
      try {
        jetty.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      jetty = null;
    }
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

  public void startSolr(Path solrHome, JettyConfig jettyConfig) {
    startSolr(solrHome, new Properties(), jettyConfig);
  }

  public void startSolr(Path solrHome, Properties nodeProps, JettyConfig jettyConfig) {
    try {
      jetty = createSolr(solrHome, nodeProps, jettyConfig);
      jetty.start();
      int port = jetty.getLocalPort();
      log.info("Jetty Assigned Port#{}", port);
      adminClient = SolrTestCaseJ4.getHttpSolrClient(jetty.getBaseUrl().toString());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private JettySolrRunner createSolr(Path solrHome, Properties nodeProps, JettyConfig jettyConfig)
      throws Exception {
    return new JettySolrRunner(solrHome.toString(), nodeProps, jettyConfig);
  }

  public JettySolrRunner getJetty() {
    return jetty;
  }

  @Override
  public SolrClient getSolrClient(String name) {
    try {
      throw new Exception("Unimplemented");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SolrClient getAdminClient() {
    return adminClient;
  }
}
