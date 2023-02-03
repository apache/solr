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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base test class for ZooKeeper tests. */
public abstract class AbstractZkTestCase extends SolrTestCaseJ4 {
  private static final String ZOOKEEPER_FORCE_SYNC = "zookeeper.forceSync";

  public static final int TIMEOUT = 45000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static Path SOLRHOME;

  static {
    try {
      SOLRHOME = SolrTestCaseJ4.TEST_PATH();
    } catch (RuntimeException e) {
      log.warn("TEST_PATH() does not exist - solrj test?");
      // solrj tests not working with TEST_HOME()
      // must override getSolrHome
    }
  }

  protected static volatile ZkTestServer zkServer;

  protected static volatile Path zkDir;

  @BeforeClass
  public static void azt_beforeClass() throws Exception {
    zkDir = createTempDir("zkData");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();

    System.setProperty("solrcloud.skip.autorecovery", "true");
    System.setProperty("zkHost", zkServer.getZkAddress());
    System.setProperty("jetty.port", "0000");
    System.setProperty(ZOOKEEPER_FORCE_SYNC, "false");

    zkServer.buildZooKeeper(SOLRHOME, "solrconfig.xml", "schema.xml");

    initCore("solrconfig.xml", "schema.xml");
  }

  @AfterClass
  public static void azt_afterClass() throws Exception {

    try {
      deleteCore();
    } finally {

      System.clearProperty("zkHost");
      System.clearProperty("solr.test.sys.prop1");
      System.clearProperty("solr.test.sys.prop2");
      System.clearProperty("solrcloud.skip.autorecovery");
      System.clearProperty("jetty.port");
      System.clearProperty(ZOOKEEPER_FORCE_SYNC);

      if (zkServer != null) {
        zkServer.shutdown();
        zkServer = null;
      }
      zkDir = null;
    }
  }
}
