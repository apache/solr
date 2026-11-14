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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests how the embedded ZooKeeper picks its client port. */
public class SolrZkServerTest extends SolrTestCase {

  private static final String ZOO_CFG =
      """
      tickTime=2000
      initLimit=10
      syncLimit=5
      """;

  private Path confHome;

  @Before
  public void createConfHome() throws Exception {
    confHome = createTempDir("zkConf");
  }

  @After
  public void clearEmbeddedZkPort() {
    System.clearProperty(SolrZkServer.EMBEDDED_ZK_PORT_PROPERTY);
  }

  @Test
  public void testClientPortInZooCfgWins() throws Exception {
    writeZooCfg("clientPort=2345\n");
    System.setProperty(SolrZkServer.EMBEDDED_ZK_PORT_PROPERTY, "3456");
    assertEquals(2345, clientPort(8983));
  }

  @Test
  public void testConfiguredPortIsUsed() throws Exception {
    writeZooCfg("");
    System.setProperty(SolrZkServer.EMBEDDED_ZK_PORT_PROPERTY, "3456");
    assertEquals(3456, clientPort(8983));
  }

  @Test
  public void testDefaultsToSolrPortPlusThousand() throws Exception {
    writeZooCfg("");
    assertEquals(9983, clientPort(8983));
  }

  @Test
  public void testInvalidConfiguredPortIsRejected() throws Exception {
    writeZooCfg("");
    for (String value : List.of("0", "65536", "-1", "not-a-port")) {
      System.setProperty(SolrZkServer.EMBEDDED_ZK_PORT_PROPERTY, value);
      SolrException e = expectThrows(SolrException.class, () -> clientPort(8983));
      assertTrue(e.getMessage().contains(SolrZkServer.EMBEDDED_ZK_PORT_PROPERTY));
    }
  }

  private void writeZooCfg(String extraProperties) throws IOException {
    Files.writeString(confHome.resolve("zoo.cfg"), ZOO_CFG + extraProperties);
  }

  private int clientPort(int solrPort) throws Exception {
    SolrZkServer zkServer =
        new SolrZkServer(true, null, createTempDir("zkData"), confHome.toString(), solrPort);
    zkServer.parseConfig();
    return zkServer.zkProps.getClientPortAddress().getPort();
  }
}
