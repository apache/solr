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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that a shard created via the Collections API stays in CONSTRUCTION state until its
 * replicas are ACTIVE, so queries never route to a half-created shard (SOLR-13136).
 */
public class CreateShardConstructionTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testNewShardStaysConstructionUntilReplicasActive() throws Exception {
    String collection = "shard_construction_test";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collection, "conf", "shard1", 1)
        .processAndWait(cluster.getSolrClient(), 60);
    cluster.waitForActiveCollection(collection, 1, 1);

    // Record every state the new shard is observed in; a ZK watcher fires on each state change
    // so the brief CONSTRUCTION window can't be missed by polling.
    Set<Slice.State> observedStates = ConcurrentHashMap.newKeySet();
    CountDownLatch activeLatch = new CountDownLatch(1);
    CollectionStateWatcher watcher =
        (liveNodes, collectionState) -> {
          if (collectionState != null) {
            Slice slice = collectionState.getSlice("shard2");
            if (slice != null) {
              observedStates.add(slice.getState());
              if (slice.getState() == Slice.State.ACTIVE
                  && slice.getReplicas().stream()
                      .allMatch(r -> r.getState() == Replica.State.ACTIVE)) {
                activeLatch.countDown();
              }
            }
          }
          return false;
        };
    ZkStateReader zkStateReader = ZkStateReader.from(cluster.getSolrClient());
    zkStateReader.registerCollectionStateWatcher(collection, watcher);
    try {
      AtomicReference<Exception> createError = new AtomicReference<>();
      Thread createThread =
          new Thread(
              () -> {
                try {
                  CollectionAdminRequest.createShard(collection, "shard2")
                      .process(cluster.getSolrClient());
                } catch (Exception e) {
                  createError.set(e);
                }
              });
      createThread.start();

      // Query while the shard is under construction; it must not fail with
      // "no servers hosting shard".
      ModifiableSolrParams queryParams = params("q", "*:*", "rows", "0");
      Exception queryError = null;
      long deadline = System.currentTimeMillis() + 120000;
      while (!activeLatch.await(200, TimeUnit.MILLISECONDS)) {
        if (System.currentTimeMillis() > deadline) {
          break;
        }
        try {
          cluster.getSolrClient().query(collection, queryParams);
        } catch (Exception e) {
          if (e.getMessage() != null && e.getMessage().contains("no servers hosting shard")) {
            queryError = e;
            break;
          }
        }
      }
      createThread.join(180000);

      assertNull("createShard failed: " + createError.get(), createError.get());
      assertNull("query failed during shard creation: " + queryError, queryError);
      assertTrue(
          "new shard was never observed in CONSTRUCTION state (saw: " + observedStates + ")",
          observedStates.contains(Slice.State.CONSTRUCTION));
      assertTrue("new shard never reached ACTIVE", activeLatch.getCount() == 0);
    } finally {
      zkStateReader.removeCollectionStateWatcher(collection, watcher);
      CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
    }
  }
}
