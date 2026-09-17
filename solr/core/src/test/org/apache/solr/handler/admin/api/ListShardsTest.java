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

import static org.hamcrest.Matchers.containsString;

import org.apache.solr.client.api.model.ListShardsResponse;
import org.apache.solr.client.api.model.ListShardsResponse.ShardSummary;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ShardsApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/** HTTP tests for {@code GET /api/collections/{collection}/shards} via generated SolrJ. */
public class ListShardsTest extends SolrCloudTestCase {

  private static final String COLLECTION = "listShardsColl";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);
  }

  @Test
  public void testListShards() throws Exception {
    final ListShardsResponse rsp =
        new ShardsApi.ListShards(COLLECTION).process(cluster.getSolrClient());

    assertNotNull(rsp);
    assertNull(rsp.error);
    assertEquals(2, rsp.shards.size());
    assertTrue(rsp.shards.containsKey("shard1"));
    assertTrue(rsp.shards.containsKey("shard2"));

    for (ShardSummary summary : rsp.shards.values()) {
      assertEquals("active", summary.state);
      assertNotNull(summary.range);
      assertFalse(summary.range.isEmpty());
      assertEquals("GREEN", summary.replicaHealth);
      assertEquals(Integer.valueOf(1), summary.replicaCount);
      assertEquals(Integer.valueOf(1), summary.activeReplicaCount);
    }
  }

  @Test
  public void testMissingCollectionReturnsNotFound() {
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class,
            () -> new ShardsApi.ListShards("doesNotExist").process(cluster.getSolrClient()));
    assertEquals(404, ex.code());
    assertThat(ex.getMessage(), containsString("Collection not found: doesNotExist"));
  }
}
