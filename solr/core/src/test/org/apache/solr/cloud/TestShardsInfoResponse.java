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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.IsIterableContaining.hasItems;

/**
 * Test which asserts that shards.info=true works even if several shards are down
 * It must return one unique key per shard.
 * See SOLR-14892
 */
public class TestShardsInfoResponse extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("cloud-minimal", configset("cloud-minimal"))
        .configure();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchingWithShardsInfoMustNotReturnEmptyOrDuplicateKeys() throws Exception {

    CollectionAdminRequest.createCollection("collection", "cloud-minimal", 3, 1)
        .process(cluster.getSolrClient());

    UpdateRequest update = new UpdateRequest();
    for (int i = 0; i < 100; i++) {
      update.add("id", Integer.toString(i));
    }
    update.commit(cluster.getSolrClient(), "collection");

    // Stops 2 shards
    for (int i = 0; i < 2; i++) {
      cluster.waitForJettyToStop(cluster.stopJettySolrRunner(i));
    }

    QueryResponse response = cluster.getSolrClient().query("collection", new SolrQuery("*:*")
        .setRows(1)
        .setParam(ShardParams.SHARDS_TOLERANT, true)
        .setParam(ShardParams.SHARDS_INFO, true));
    assertEquals(0, response.getStatus());
    assertTrue(response.getResults().getNumFound() > 0);

    SimpleOrderedMap<Object> shardsInfo = (SimpleOrderedMap<Object>) response.getResponse().get("shards.info");
    // We verify that there are no duplicate keys in case of 2 shards in error
    assertEquals(3, shardsInfo.size());

    Collection<String> keys = new ArrayList<>();
    keys.add(shardsInfo.getName(0));
    keys.add(shardsInfo.getName(1));
    keys.add(shardsInfo.getName(2));

    MatcherAssert.assertThat((Iterable<String>) keys, hasItems("unknown_shard_1", "unknown_shard_2"));
  }
}
