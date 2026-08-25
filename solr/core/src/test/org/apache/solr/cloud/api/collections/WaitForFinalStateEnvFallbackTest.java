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
package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.AsyncCollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaitForFinalStateEnvFallbackTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @After
  public void releaseInjectionAndProperty() {
    TestInjection.prepRecoveryOpPauseForever = null;
    TestInjection.notifyPauseForeverDone();
    System.clearProperty(CommonAdminParams.WAIT_FOR_FINAL_STATE_DEFAULT_PROP);
  }

  private static AsyncCollectionAdminRequest addReplicaWithTimeout(
      String collection, String shard, int timeoutSeconds) {
    return new AsyncCollectionAdminRequest(CollectionAction.ADDREPLICA) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
        params.set(CoreAdminParams.COLLECTION, collection);
        params.set(CoreAdminParams.SHARD, shard);
        params.set(CommonAdminParams.TIMEOUT, timeoutSeconds);
        return params;
      }
    };
  }

  @Test
  public void testDefaultFalseSkipsWaitEvenWhenRecoveryIsStuck() throws Exception {
    String collection = "envfallbackfalse";
    SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1).process(client);
    cluster.waitForActiveCollection(collection, 1, 1);

    // no system property set: ADDREPLICA's literal default is false (waiting on a replica's
    // recovery is unbounded in time, unlike CREATE/CREATESHARD/SPLITSHARD's brand-new replicas)
    TestInjection.prepRecoveryOpPauseForever = "true:100";

    long start = System.nanoTime();
    addReplicaWithTimeout(collection, "shard1", 5).process(client);
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    log.info("ADDREPLICA with default (false) returned after {}ms", elapsedMs);

    // proves we returned without waiting on the (permanently stuck) recovery: if the
    // ActiveReplicaWatcher had been registered with the 5s timeout above, a genuine wait would
    // either finish fast (if it wasn't really stuck) or throw after ~5s -- neither of which we
    // want; we want no watcher registered at all, so this returns near-instantly.
    assertTrue(
        "expected ADDREPLICA to return promptly since waitForFinalState defaults to false, "
            + "but it took "
            + elapsedMs
            + "ms",
        elapsedMs < 5_000);

    DocCollection coll = cluster.getSolrClient().getClusterState().getCollection(collection);
    boolean anyNonActive =
        coll.getReplicas().stream().anyMatch(r -> r.getState() != Replica.State.ACTIVE);
    assertTrue(
        "expected the new replica to still be stuck in recovery (not ACTIVE) since we "
            + "returned before waiting for final state",
        anyNonActive);
  }

  @Test
  public void testEnvFallbackTrueActuallyWaitsAndTimesOutWhenRecoveryIsStuck() throws Exception {
    String collection = "envfallbacktrue";
    SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1).process(client);
    cluster.waitForActiveCollection(collection, 1, 1);

    System.setProperty(CommonAdminParams.WAIT_FOR_FINAL_STATE_DEFAULT_PROP, "true");
    TestInjection.prepRecoveryOpPauseForever = "true:100";

    long start = System.nanoTime();
    boolean threw = false;
    try {
      addReplicaWithTimeout(collection, "shard1", 5).process(client);
    } catch (Exception e) {
      threw = true;
      log.info("ADDREPLICA with env fallback=true threw as expected: {}", e);
    }
    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
    log.info("ADDREPLICA with env fallback=true returned/threw after {}ms", elapsedMs);

    assertTrue(
        "expected ADDREPLICA to actually wait for final state (and time out, since recovery "
            + "is permanently stuck) when waitForFinalState resolves to true via the env "
            + "fallback, but it returned successfully after only "
            + elapsedMs
            + "ms",
        threw);
  }
}
