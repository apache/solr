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

package org.apache.solr.core;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;


public class SolrCoreConfigTest extends SolrCloudTestCase {
  @Override
  public void setUp() throws Exception {
    System.setProperty(SolrCore.DISABLE_ZK_CONFIG_WATCH, "true");
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    try {
      super.tearDown();
    } finally {
      System.clearProperty(SolrCore.DISABLE_ZK_CONFIG_WATCH);
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
            .addConfig("conf", configset("cloud-minimal"))
            .configure();
  }

  @Test
  public void testNoZkConfigWatch() {
    CoreContainer cc = getContainer();
    assertFalse(
        "There shouldn't be any conf listener",
        cc.getZkController().hasConfDirectoryListeners("/configs/conf1"));
  }

  private CoreContainer getContainer() {
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
    for (JettySolrRunner jetty : jettys) {
      return jetty.getCoreContainer();
    }
    return null;
  }
}
