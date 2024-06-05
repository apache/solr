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
package org.apache.solr.client.solrj.impl;

import java.util.Set;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider.RemoteCallCtx;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeValueFetcherTest extends SolrCloudTestCase {

  private static final String COLLECTION =
      NodeValueFetcherTest.class.getSimpleName() + "_collection";

  private static int numShards;
  private static int numReplicas;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Metrics should be enabled to get them with snitch
    System.setProperty("metricsEnabled", "true");

    numShards = random().nextInt(3) + 3;
    numReplicas = random().nextInt(2) + 2;
    int numNodes = random().nextInt(2) + 1;

    configureCluster(numNodes).configure();
  }

  @Before
  public void setupCollection() throws Exception {
    CollectionAdminResponse rsp =
        CollectionAdminRequest.createCollection(COLLECTION, numShards, numReplicas)
            .process(cluster.getSolrClient());
    assertTrue(rsp.isSuccess());
    cluster.waitForActiveCollection(COLLECTION, numShards, numShards * numReplicas);
  }

  @After
  public void cleanup() throws Exception {
    cluster.deleteAllCollections();
  }

  @Test
  public void testGetTags() {

    CloudLegacySolrClient solrClient = (CloudLegacySolrClient) cluster.getSolrClient();
    int totalCores = 0;

    // Sum all the cores of the collection by fetching tags of all nodes.
    // We should get same number than when we created the collection
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      String node = runner.getNodeName();
      RemoteCallCtx ctx = new RemoteCallCtx(node, solrClient);
      NodeValueFetcher fetcher = new NodeValueFetcher();

      Set<String> requestedTags = Set.of("cores");
      fetcher.getTags(requestedTags, ctx);

      // make sure we only get the tag we asked
      assertEquals(1, ctx.tags.size());

      int coresOnNode = (Integer) ctx.tags.get("cores");
      totalCores += coresOnNode;
    }

    assertEquals(numShards * numReplicas, totalCores);
  }
}
