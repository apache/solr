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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.embedded.JettySolrRunner;
import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.core.IsIterableContaining.hasItems;
/**
 * Test which asserts that shards.info=true works even if several shards are down It must return one
 * unique key per shard. See SOLR-14892
 */
public class TestShardsInfoResponse3 extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("cloud-minimal", configset("cloud-minimal")).configure();
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

    FileFilter isDir = ff -> ff.isDirectory();
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      File solrHome = new File(jetty.getSolrHome());
      if (solrHome.listFiles(isDir).length == 2) {
        log.info("Stopping  " + jetty.toString());
      }
    }

    QueryResponse response =
        cluster
            .getSolrClient()
            .query(
                "collection",
                new SolrQuery("*:*")
                    .setRows(1)
                    .setParam(ShardParams.SHARDS_TOLERANT, true)
                    .setParam(ShardParams.SHARDS_INFO, true));
    assertEquals(0, response.getStatus());
    assertTrue(response.getResults().getNumFound() > 0);

    SimpleOrderedMap<Object> shardsInfo =
        (SimpleOrderedMap<Object>) response.getResponse().get("shards.info");
    // We verify that there are no duplicate keys in case of 2 shards in error
    assertEquals(3, shardsInfo.size());

    Collection<String> keys = new ArrayList<>();
    keys.add(shardsInfo.getName(0));
    keys.add(shardsInfo.getName(1));
    keys.add(shardsInfo.getName(2));

    // The names of the shards in error are generated as unknown_shard_1 and unknown_shard_2 because we could not get the real shard names.
    MatcherAssert.assertThat(
        (Iterable<String>) keys, hasItems("unknown_shard_1", "unknown_shard_2"));
  }
}
