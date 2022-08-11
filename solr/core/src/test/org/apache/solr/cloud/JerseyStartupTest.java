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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.V2Response;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** */
public class JerseyStartupTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    /*
    Handler fh = new FileHandler("/tmp/jersey_test.log");
    Logger.getLogger("").addHandler(fh);
    Logger.getLogger("org.glassfish.jersey").setLevel(Level.FINEST); */

    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    cluster.waitForAllNodes(30);
    cluster.deleteAllCollections();
  }

  @Test
  public void testDebugJerseyStartup() throws Exception {
    final SolrClient client = cluster.getSolrClient();

    try{
      final V2Response resp2 = new V2Request.Builder("/api/someadminpath").GET().build().process(client);
      System.out.println(resp2);
    } catch (Exception e) {
      e.printStackTrace();
    }

    String collection = "testAddMultipleReplicas";
  }
}
