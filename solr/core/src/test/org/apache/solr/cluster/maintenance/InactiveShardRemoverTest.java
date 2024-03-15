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

package org.apache.solr.cluster.maintenance;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.ShardTestUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.junit.BeforeClass;
import org.junit.Test;

public class InactiveShardRemoverTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testDeleteInactiveShard() throws Exception {

    addPlugin(new InactiveShardRemoverConfig(1, 0, 2));
    try {
      final String collectionName = "testDeleteInactiveShard";
      createCollection(collectionName, 1);

      final String sliceName =
          new ArrayList<>(getCollectionState(collectionName).getSlices()).get(0).getName();
      ShardTestUtil.setSliceState(cluster, collectionName, sliceName, Slice.State.INACTIVE);

      waitForState(
          "Waiting for inactive shard to be deleted",
          collectionName,
          clusterShape(0, 0),
          5,
          TimeUnit.SECONDS);
    } finally {
      removePlugin();
    }
  }

  @Test
  public void testTtl() throws Exception {

    final int ttlSeconds = 1 + random().nextInt(5);
    final TimeSource timeSource = cluster.getOpenOverseer().getSolrCloudManager().getTimeSource();

    addPlugin(new InactiveShardRemoverConfig(1, ttlSeconds, 1));
    try {
      final String collectionName = "testTtl";
      createCollection(collectionName, 1);

      final String sliceName =
          new ArrayList<>(getCollectionState(collectionName).getSlices()).get(0).getName();
      ShardTestUtil.setSliceState(cluster, collectionName, sliceName, Slice.State.INACTIVE);
      waitForState(
          "Expected shard " + sliceName + " to be in state " + Slice.State.INACTIVE,
          collectionName,
          (n, c) -> c.getSlice(sliceName).getState() == Slice.State.INACTIVE);

      final long ttlStart = timeSource.getTimeNs();

      waitForState(
          "Waiting for InactiveShardRemover to delete inactive shard",
          collectionName,
          clusterShape(0, 0),
          ttlSeconds + 5,
          TimeUnit.SECONDS);

      final long ttlEnd = timeSource.getTimeNs();
      final long ttlPeriodSeconds = TimeUnit.NANOSECONDS.toSeconds(ttlEnd - ttlStart);

      assertTrue(ttlPeriodSeconds >= ttlSeconds);
    } finally {
      removePlugin();
    }
  }

  public void testMaxShardsToDeletePerCycle() throws Exception {

    final CoreContainer cc = cluster.getOpenOverseer().getCoreContainer();

    final int maxDeletesPerCycle = 5;
    final InactiveShardRemover remover = new InactiveShardRemover(cc);
    remover.configure(new InactiveShardRemoverConfig(1, 0, maxDeletesPerCycle));

    // Remove across multiple collections
    final String collection1 = "testMaxShardsToDeletePerCycle-1";
    final String collection2 = "testMaxShardsToDeletePerCycle-2";
    final int shardsPerCollection = 10;
    final int totalShards = 2 * shardsPerCollection;

    createCollection(collection1, shardsPerCollection);
    createCollection(collection2, shardsPerCollection);

    setAllShardsInactive(collection1);
    setAllShardsInactive(collection2);

    int cycle = 0;
    int shardsDeleted = 0;
    while (shardsDeleted < totalShards) {
      cycle++;
      remover.deleteInactiveSlices();
      DocCollection coll1 = getCollectionState(collection1);
      DocCollection coll2 = getCollectionState(collection2);

      int remainingShards = coll1.getSlices().size() + coll2.getSlices().size();
      if (remainingShards != totalShards - maxDeletesPerCycle * cycle) {
        System.out.println(coll1);
        System.out.println(coll2);
      }
      assertEquals(totalShards - maxDeletesPerCycle * cycle, remainingShards);
      shardsDeleted = totalShards - remainingShards;
    }
  }

  @Test
  public void testConfigValidation() {

    try {
      new InactiveShardRemoverConfig(0, 0, 1).validate();
      fail("Expected validation error for scheduleIntervalSeconds=0");
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }

    try {
      new InactiveShardRemoverConfig(1, 0, 0).validate();
      fail("Expected validation error for maxDeletesPerCycle=0");
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }
  }

  private static void addPlugin(final InactiveShardRemoverConfig config)
      throws SolrServerException, IOException {
    PluginMeta plugin = pluginMeta(config);
    pluginRequest(Collections.singletonMap("add", plugin));
  }

  private static void removePlugin() throws SolrServerException, IOException {
    pluginRequest(Collections.singletonMap("remove", InactiveShardRemover.PLUGIN_NAME));
  }

  private static void pluginRequest(Map<String, Object> payload)
      throws SolrServerException, IOException {
    V2Request req =
        new V2Request.Builder("/cluster/plugin").withMethod(POST).withPayload(payload).build();
    V2Response rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.getStatus());
  }

  private static PluginMeta pluginMeta(final InactiveShardRemoverConfig config) {
    PluginMeta plugin = pluginMeta();
    plugin.config = config;
    return plugin;
  }

  private static PluginMeta pluginMeta() {
    PluginMeta plugin = new PluginMeta();
    plugin.klass = InactiveShardRemover.class.getName();
    plugin.name = InactiveShardRemover.PLUGIN_NAME;
    return plugin;
  }

  private void createCollection(final String collectionName, final int numShards)
      throws SolrServerException, IOException {
    CollectionAdminRequest.createCollection(collectionName, "conf", numShards, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, numShards, numShards);
  }

  private void setAllShardsInactive(final String collectionName) {
    DocCollection collection = getCollectionState(collectionName);
    collection.getSlices().stream()
        .filter(s -> s.getState() != Slice.State.INACTIVE)
        .forEach(
            s -> {
              try {
                ShardTestUtil.setSliceState(
                    cluster, s.getCollection(), s.getName(), Slice.State.INACTIVE);
                waitForState(
                    "Expected shard " + s + " to be in state " + Slice.State.INACTIVE,
                    collection.getName(),
                    (n, c) -> c.getSlice(s.getName()).getState() == Slice.State.INACTIVE);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }
}
