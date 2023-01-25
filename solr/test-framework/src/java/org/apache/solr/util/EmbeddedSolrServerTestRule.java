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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.update.UpdateShardHandlerConfig;

/**
 * Provides an EmbeddedSolrServer for tests. It starts and stops the server and provides methods for
 * creating collections and interacting with the server.
 */
public class EmbeddedSolrServerTestRule extends SolrClientTestRule {

  private static final String CORE_DIR_PROP = "coreRootDirectory";
  private EmbeddedSolrServer adminClient = null;
  private CoreContainer container = null;
  private boolean clearCoreDirSysProp = false;

  /** Provides an EmbeddedSolrServer instance for administration actions */
  public EmbeddedSolrServer getAdminClient() {
    return adminClient;
  }

  /**
   * Starts the Solr server with the given solrHome. If solrHome contains a solr.xml file, it is
   * used to configure the server. If not, a new NodeConfig is built with default settings for
   * configuration.
   */
  public void startSolr(Path solrHome) {
    NodeConfig nodeConfig;
    if (Files.exists(solrHome.resolve(SolrXmlConfig.SOLR_XML_FILE))) {
      // existing solr.xml; perhaps not recommended for new/most tests

      // solr.xml coreRootDirectory is best set to a temp directory in a test so that
      //  (a) we don't load existing cores
      //      Because it's better for tests to explicitly create cores.
      //  (b) we don't write data in the test to a likely template directory
      //  But a test can insist on something if it sets the property.
      if (System.getProperty(CORE_DIR_PROP) == null) {
        clearCoreDirSysProp = true;
        System.setProperty(CORE_DIR_PROP, LuceneTestCase.createTempDir("cores").toString());
      }

      nodeConfig = SolrXmlConfig.fromSolrHome(solrHome, null);
    } else {
      // test oriented config (preferred)
      nodeConfig = newNodeConfigBuilder(solrHome).build();
    }

    startSolr(nodeConfig);
  }

  /** Starts Solr with custom NodeConfig */
  public void startSolr(NodeConfig nodeConfig) {
    container = new CoreContainer(nodeConfig);
    container.load();
    adminClient = new EmbeddedSolrServer(container, null);
  }

  /** Returns a NodeConfigBuilder with default settings for test configuration */
  public NodeConfig.NodeConfigBuilder newNodeConfigBuilder(Path solrHome) {

    return new NodeConfig.NodeConfigBuilder("testNode", solrHome)
        .setUpdateShardHandlerConfig(UpdateShardHandlerConfig.TEST_DEFAULT)
        .setCoreRootDirectory(LuceneTestCase.createTempDir("cores").toString());
  }

  protected void create(NewCollectionBuilder b) throws SolrServerException, IOException {

    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName(b.getName());
    req.setInstanceDir(b.getName());

    if (b.getConfigSet() != null) {
      req.setConfigSet(b.getConfigSet());
    }

    /** Setting config.xml */
    if (b.getConfigFile() != null) {
      req.setConfigName(b.getConfigFile());
    }

    /** Setting schema.xml */
    if (b.getSchemaFile() != null) {
      req.setSchemaName(b.getSchemaFile());
    }

    req.process(adminClient);
  }

  /**
   * Shuts down the EmbeddedSolrServer instance and clears the coreRootDirectory system property if
   * necessary
   */
  @Override
  protected void after() {
    if (container != null) container.shutdown();

    if (clearCoreDirSysProp) {
      System.clearProperty(CORE_DIR_PROP);
    }
  }

  /** Returns EmbeddedSolrServer instance for the collection named "collection1" */
  @Override
  public EmbeddedSolrServer getSolrClient() {
    return getSolrClient("collection1");
  }

  @Override
  public EmbeddedSolrServer getSolrClient(String name) {
    return new EmbeddedSolrServer(adminClient.getCoreContainer(), name);
  }
}
