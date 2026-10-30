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

import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ShardsApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test cases for the v2 {@link SplitShard} API, driven through the generated SolrJ client. */
public class SplitShardAPITest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testSplitShard() throws Exception {
    final String collection = "splitShardAPITestColl";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);

    final ShardsApi.SplitShard splitRequest = new ShardsApi.SplitShard(collection, "shard1");
    splitRequest.setNumSubShards(2);
    assertEquals(0, splitRequest.process(cluster.getSolrClient()).responseHeader.status);

    waitForState(
        "Expected sub-shards shard1_0 and shard1_1 to be active",
        collection,
        c -> c != null && isActive(c.getSlice("shard1_0")) && isActive(c.getSlice("shard1_1")));
  }

  @Test
  public void testSplitShardWithRanges() throws Exception {
    final String collection = "splitShardRangesTestColl";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);

    // An uneven split of the parent range, so the sub-shard ranges can only match if the
    // 'ranges' value reached the split command unchanged.
    final ShardsApi.SplitShard splitRequest = new ShardsApi.SplitShard(collection, "shard1");
    splitRequest.setRanges("80000000-bfffffff,c0000000-7fffffff");
    assertEquals(0, splitRequest.process(cluster.getSolrClient()).responseHeader.status);

    waitForState(
        "Expected sub-shards shard1_0 and shard1_1 to be active",
        collection,
        c -> c != null && isActive(c.getSlice("shard1_0")) && isActive(c.getSlice("shard1_1")));

    final var docCollection = getCollectionState(collection);
    assertEquals(0x80000000, docCollection.getSlice("shard1_0").getRange().min);
    assertEquals(0xbfffffff, docCollection.getSlice("shard1_0").getRange().max);
    assertEquals(0xc0000000, docCollection.getSlice("shard1_1").getRange().min);
    assertEquals(0x7fffffff, docCollection.getSlice("shard1_1").getRange().max);
  }

  @Test
  public void testSplitShardAsync() throws Exception {
    final String collection = "splitShardAsyncTestColl";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);

    final ShardsApi.SplitShard splitRequest = new ShardsApi.SplitShard(collection, "shard1");
    splitRequest.setNumSubShards(2);
    splitRequest.setAsync("splitShardAsyncTestId");
    final var response = splitRequest.process(cluster.getSolrClient());
    assertEquals(0, response.responseHeader.status);
    assertEquals("splitShardAsyncTestId", response.requestId);

    waitForState(
        "Expected sub-shards shard1_0 and shard1_1 to be active",
        collection,
        c -> c != null && isActive(c.getSlice("shard1_0")) && isActive(c.getSlice("shard1_1")));
  }

  @Test
  public void testSplitMissingCollectionThrows() {
    final ShardsApi.SplitShard splitRequest =
        new ShardsApi.SplitShard("missingCollection", "shard1");
    final RemoteSolrException ex =
        expectThrows(
            RemoteSolrException.class, () -> splitRequest.process(cluster.getSolrClient()));
    assertTrue("Expected an error status, got " + ex.code(), ex.code() >= 400);
  }

  private static boolean isActive(Slice slice) {
    return slice != null && slice.getState() == Slice.State.ACTIVE;
  }
}
