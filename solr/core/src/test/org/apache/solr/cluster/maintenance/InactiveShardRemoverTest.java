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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.junit.AfterClass;
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

  @AfterClass
  public static void tearDownCluster() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testDeleteInactiveShard() throws Exception {

    addPlugin(new InactiveShardRemoverConfig(1, 0, 2));
    try {
      final String collectionName = "testDeleteInactiveShard";
      createCollection(collectionName, 1);

      final String sliceName =
          new ArrayList<>(getCollectionState(collectionName).getSlices()).get(0).getName();
      setSliceState(collectionName, sliceName, Slice.State.INACTIVE);

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
      setSliceState(collectionName, sliceName, Slice.State.INACTIVE);

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
    final int shardsPerCollection = 10;
    final int maxDeletesPerCycle = 5;
    final Set<String> asyncIds = new HashSet<>();

    // This test uses an anonymous actor to delete shards synchronously
    InactiveShardRemover remover =
        new InactiveShardRemover(
            cc,
            new InactiveShardRemover.DeleteActor(cc) {
              @Override
              void delete(final Slice slice, final String asyncId) throws IOException {
                try {
                  asyncIds.add(asyncId);
                  CollectionAdminRequest.deleteShard(slice.getCollection(), slice.getName())
                      .process(cluster.getSolrClient());
                } catch (SolrServerException e) {
                  throw new RuntimeException(e);
                }
              }
            });

    remover.configure(new InactiveShardRemoverConfig(0, 0, maxDeletesPerCycle));

    final String collection1 = "testMaxShardsToDeletePerCycle-1";
    final String collection2 = "testMaxShardsToDeletePerCycle-2";

    createCollection(collection1, shardsPerCollection);
    createCollection(collection2, shardsPerCollection);

    setAllShardsInactive(collection1);
    setAllShardsInactive(collection2);

    int shardsDeleted = 0;

    while (shardsDeleted < shardsPerCollection * 2) {
      remover.deleteInactiveSlices();

      assertEquals(asyncIds.size(), shardsDeleted + maxDeletesPerCycle);
      shardsDeleted = asyncIds.size();
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
                setSliceState(s.getCollection(), s.getName(), Slice.State.INACTIVE);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }
}
