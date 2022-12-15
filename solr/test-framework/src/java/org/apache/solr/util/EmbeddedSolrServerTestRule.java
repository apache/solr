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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedSolrServerTestRule extends SolrClientTestRule {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String DEFAULT_CORE_NAME = "collection1";

  public  Path solrHome;

  public EmbeddedSolrServer client = null;

  public EmbeddedSolrServerTestRule() {

  }

  public void init(Path home) {
    assert (client == null);
    solrHome = home;
    client = new EmbeddedSolrServer(solrHome, DEFAULT_CORE_NAME);
  }

  /*public void init2(Path home) {

    solrHome = home;


    // final String home = SolrJettyTestBase.legacyExampleCollection1SolrHome();
    final String config = solrHome + "/" + DEFAULT_CORE_NAME + "/conf/solrconfig.xml";
    final String schema = solrHome + "/" + DEFAULT_CORE_NAME + "/conf/schema.xml";
    System.setProperty("solr.solr.home", solrHome.toString());
    log.info("####initCore");

    // SolrTestCaseJ4.ignoreException("ignore_exception");
    // SolrTestCaseJ4.factoryProp = System.getProperty("solr.directoryFactory");
    // if (SolrTestCaseJ4.factoryProp == null) {
    //  System.setProperty("solr.directoryFactory", "solr.RAMDirectoryFactory");
    // }

    // other  methods like starting a jetty instance need these too
    // System.setProperty("solr.test.sys.prop1", "propone");
    // System.setProperty("solr.test.sys.prop2", "proptwo");

    String configFile = SolrTestCaseJ4.getSolrConfigFile();
    if (configFile != null) {

      // Creating core
      SolrTestCaseJ4.createCore();
    }
    log.info("####initCore end");

  h =
        new TestHarness(
            DEFAULT_CORE_NAME,
            initAndGetDataDir().getAbsolutePath(),
            "solrconfig.xml",
            getSchemaFile());

  client = new EmbeddedSolrServer(solrHome, DEFAULT_CORE_NAME);
  }
*/

  @Override
  protected void before() throws Throwable {
    super.before();

  }

  @Override
  protected void after() {

    try {
      client.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
  @Override
  public SolrClient getSolrClient() {
    return client;
  }

  @Override
  public void clearIndex() throws SolrServerException, IOException {
   client.deleteByQuery("*:*");
  }

  @Override
  public Path getSolrHome() {
    return solrHome;
  }




/*  protected static File initAndGetDataDir() {
    File dataDir = null;
    if (null == dataDir) {
      final int id = dataDirCount.incrementAndGet();
      dataDir = createTempDir("data-dir-" + id).toFile();
      assertNotNull(dataDir);
      if (log.isInfoEnabled()) {
        log.info("Created dataDir: {}", dataDir.getAbsolutePath());
      }
    }
    return dataDir;
  }*/



  /*  public static void initCore() throws Exception {
    final String home = SolrJettyTestBase.legacyExampleCollection1SolrHome();
    final String config = home + "/" + DEFAULT_CORE_NAME + "/conf/solrconfig.xml";
    final String schema = home + "/" + DEFAULT_CORE_NAME + "/conf/schema.xml";
    Assert.assertNotNull(home);
    // SolrTestCaseJ4.configString = config;
    // SolrTestCaseJ4.schemaString = schema;
    // SolrTestCaseJ4.testSolrHome = Paths.get(home);
    // System.setProperty("solr.solr.home", home);
    log.info("####initCore");

    SolrTestCaseJ4.ignoreException("ignore_exception");
    // SolrTestCaseJ4.factoryProp = System.getProperty("solr.directoryFactory");
    // if (SolrTestCaseJ4.factoryProp == null) {
    // System.setProperty("solr.directoryFactory", "solr.RAMDirectoryFactory");
    // }

    // other  methods like starting a jetty instance need these too
    // System.setProperty("solr.test.sys.prop1", "propone");
    // System.setProperty("solr.test.sys.prop2", "proptwo");

    String configFile = SolrTestCaseJ4.getSolrConfigFile();
    if (configFile != null) {
      SolrTestCaseJ4.createCore();
    }
    log.info("####initCore end");
  }*/


}
