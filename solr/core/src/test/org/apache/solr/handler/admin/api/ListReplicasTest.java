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
package org.apache.solr.handler.admin.api;

import org.apache.solr.client.api.model.ListReplicasResponse;
import org.apache.solr.client.api.model.ReplicaInfo;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ReplicasApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/** HTTP tests for {@code GET /api/collections/{collection}/shards/{shard}/replicas}. */
public class ListReplicasTest extends SolrCloudTestCase {

  private static final String COLLECTION = "listReplicasColl";
  private static final String SHARD = "shard1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 2);
  }

  @Test
  public void testListReplicas() throws Exception {
    ListReplicasResponse rsp =
        new ReplicasApi.ListReplicas(COLLECTION, SHARD).process(cluster.getSolrClient());

    assertNotNull(rsp);
    assertNull(rsp.error);
    assertNotNull(rsp.replicas);
    assertEquals(2, rsp.replicas.size());

    int leaders = 0;
    for (ReplicaInfo replica : rsp.replicas.values()) {
      assertNotNull(replica.core);
      assertNotNull(replica.baseUrl);
      assertNotNull(replica.nodeName);
      assertEquals("active", replica.state);
      assertEquals("NRT", replica.type);
      if (Boolean.TRUE.equals(replica.leader)) {
        leaders++;
      }
    }
    assertEquals(1, leaders);
  }

  @Test
  public void testUnknownCollectionReturns404() {
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () ->
                new ReplicasApi.ListReplicas("does-not-exist", SHARD)
                    .process(cluster.getSolrClient()));
    assertEquals(404, ex.code());
    assertTrue(ex.getMessage().contains("Collection: does-not-exist not found"));
  }

  @Test
  public void testUnknownShardReturns404() {
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () ->
                new ReplicasApi.ListReplicas(COLLECTION, "missingShard")
                    .process(cluster.getSolrClient()));
    assertEquals(404, ex.code());
    assertTrue(ex.getMessage().contains("shard: missingShard not found"));
  }
}
