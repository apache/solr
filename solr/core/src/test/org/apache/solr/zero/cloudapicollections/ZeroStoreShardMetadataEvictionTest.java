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
package org.apache.solr.zero.cloudapicollections;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.apache.solr.zero.process.ZeroStoreManager;
import org.apache.solr.zero.process.ZeroStoreSolrCloudTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * Tests related to the loading and unloading of SolrCores with respect to replicas of type ZERO.
 */
public class ZeroStoreShardMetadataEvictionTest extends ZeroStoreSolrCloudTestCase {

  String collectionName = "ZeroCollection";
  int numReplicas = 1;
  String shardNames = "shard1";

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    resetFactory();
  }

  /**
   * Verifies that creating a collection and deleting it will evict any pre-existing cached core
   * metadata information from {@link MetadataCacheManager}. See SOLR-14134
   */
  @Test
  public void testDeleteCollectionEvictsExistingMetadata() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    assertEvictionOnCollectionCommand(
        () -> {
          // delete the collection
          try {
            CollectionAdminRequest.Delete delete =
                CollectionAdminRequest.deleteCollection(collectionName);
            delete.process(cloudClient).getResponse();
            cluster
                .getZkStateReader()
                .waitForState(collectionName, 60, TimeUnit.SECONDS, Objects::isNull);
          } catch (Exception e) {
            fail(e.getMessage());
          }
        });
  }

  /**
   * Verifies that creating a collection and deleting its shard will evict any pre-existing cached
   * Zero store metadata information
   */
  @Test
  public void testDeleteShardEvictsExistingMetadata() throws Exception {
    setupCluster(1);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    assertEvictionOnCollectionCommand(
        () -> {
          // delete the shard
          try {
            CollectionAdminRequest.DeleteShard delete =
                CollectionAdminRequest.deleteShard(collectionName, shardNames);
            delete.process(cloudClient).getResponse();
            cluster
                .getZkStateReader()
                .waitForState(
                    collectionName, 60, TimeUnit.SECONDS, (c) -> c.getSlice(shardNames) == null);
          } catch (Exception e) {
            fail(e.getMessage());
          }
        });
  }

  // helper to assert a collection command that deletes shards will evict Zero store metadata for
  // the replica belonging to that shard
  private void assertEvictionOnCollectionCommand(Runnable collCmd) throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // setup testing components
    AtomicInteger evictionCount = new AtomicInteger(0);
    MetadataCacheManager metadataCacheManager =
        configureTestMetadataCacheManager(cluster.getJettySolrRunner(0), evictionCount);

    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // do an indexing request to populate the cache entry
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.process(cloudClient, collectionName);

    CoreContainer coreContainer = cluster.getJettySolrRunner(0).getCoreContainer();
    // get the single replica for the collection and verify its cache entry has been populated
    Replica shardLeaderReplica = getShardLeaderReplica(collectionName);
    MetadataCacheManager.MetadataCacheEntry coreMetadata =
        metadataCacheManager.getOrCreateCoreMetadata(shardLeaderReplica.getCoreName());

    // there should be only one update and no evictions
    assertEquals(1, coreMetadata.getMetadataVersion().getVersion());
    assertEquals(0, evictionCount.get());

    // run command
    collCmd.run();

    // Trying to retrieve the MetadataCacheEntry should init it again at the default values
    // (see MetadataCacheManager.getOrCreateCoreMetadata())
    coreMetadata = metadataCacheManager.getOrCreateCoreMetadata(shardLeaderReplica.getCoreName());

    assertEquals(1, evictionCount.get());
    assertEquals(0, coreMetadata.getMetadataVersion().getVersion());
    assertEquals("iNiTiAl", coreMetadata.getMetadataVersion().getMetadataSuffix());

    ZeroStoreShardMetadata shardMetadata = coreMetadata.getZeroShardMetadata();
    // verify the shardMetadata is at the default generation
    ZeroStoreShardMetadata emptyShardMetadata = new ZeroStoreShardMetadata();
    assertNotNull(shardMetadata);
    assertEquals(emptyShardMetadata.getGeneration(), shardMetadata.getGeneration());
  }

  private MetadataCacheManager configureTestMetadataCacheManager(
      JettySolrRunner runner, AtomicInteger evictionCount) {
    ZeroStoreManager manager = runner.getCoreContainer().getZeroStoreManager();
    MetadataCacheManager metadataCacheManager =
        new MetadataCacheManager(manager.getConfig(), runner.getCoreContainer()) {

          @Override
          public boolean removeCoreMetadata(String coreName) {
            if (super.removeCoreMetadata(coreName)) {
              evictionCount.incrementAndGet();
              return true;
            }
            return false;
          }
        };
    setupTestMetadataCacheManager(metadataCacheManager, runner);
    return metadataCacheManager;
  }

  // takes a single shard name
  private Replica getShardLeaderReplica(String collectionName) {
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);
    return collection.getLeader(shardNames);
  }
}
