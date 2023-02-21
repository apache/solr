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

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.apache.solr.SolrTestCaseJ4.DEFAULT_TEST_CORENAME;
import static org.apache.solr.SolrTestCaseJ4.getHttpSolrClient;
import static org.apache.solr.SolrTestCaseJ4.initCore;
import static org.apache.solr.SolrTestCaseJ4.writeCoreProperties;

import java.io.IOException;
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
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    adminClient = null;

    if (jetty != null) {
      try {
        jetty.stop();
      } catch (Exception e) {
        try {
          throw new IOException(e);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
      jetty = null;
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

  public void startSolr(Path solrHome, JettyConfig jettyConfig) {
    startSolr(solrHome, new Properties(), jettyConfig);
  }

  public void startSolr(Path solrHome, Properties nodeProperties, JettyConfig jettyConfig) {

    try {
      initCore(null, null, solrHome.toString());

      Path coresDir = createTempDir().resolve("cores");

      Properties props = new Properties();
      props.setProperty("name", DEFAULT_TEST_CORENAME);
      props.setProperty("configSet", "collection1");
      props.setProperty("config", "${solrconfig:solrconfig.xml}");
      props.setProperty("schema", "${schema:schema.xml}");

      writeCoreProperties(coresDir.resolve("core"), props, "RestTestBase");

      Properties nodeProps = new Properties(nodeProperties);
      nodeProps.setProperty("coreRootDirectory", coresDir.toString());
      nodeProps.setProperty("configSetBaseDir", solrHome.toString());

      jetty = new JettySolrRunner(solrHome.toString(), nodeProps, jettyConfig);
      jetty.start();
      int port = jetty.getLocalPort();
      log.info("Jetty Assigned Port#{}", port);
      adminClient = getHttpSolrClient(jetty.getBaseUrl().toString());
    } catch (Exception e) {
      try {
        throw new IOException();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public JettySolrRunner getJetty() {
    if (jetty == null) throw new IllegalStateException("Jetty has not started");
    return jetty;
  }

  @Override
  public SolrClient getSolrClient(String name) {
    return getHttpSolrClient(jetty.getBaseUrl().toString() + "/" + DEFAULT_TEST_CORENAME);
  }

  @Override
  public SolrClient getAdminClient() {
    return adminClient;
  }
}
