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
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
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

  // unique per invocation so the class can be stress-run with -Ptests.iters, which a fixed
  // collection name makes impossible: every repeat fails instantly on "collection already exists"
  private static final AtomicInteger runId = new AtomicInteger();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @After
  public void releaseInjectionAndProperty() {
    // reset() clears the injection, releases any paused op and zeroes the pause counter.
    // That counter matters: TestInjection pauses only ONE prep-recovery op at a time ("Prevent for
    // continuous pause forever"), so a replica left recovering by an earlier test can consume the
    // single slot and the next test's ADDREPLICA is never paused -- which made these assertions
    // pass or fail by test order.
    TestInjection.reset();
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
    String collection = "envfallbackfalse" + runId.incrementAndGet();
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
    boolean anyNonActive = coll.replicaStream().anyMatch(r -> r.getState() != Replica.State.ACTIVE);
    assertTrue(
        "expected the new replica to still be stuck in recovery (not ACTIVE) since we "
            + "returned before waiting for final state",
        anyNonActive);
  }

  /**
   * The other two cases exercise the property unset and set to {@code "true"}; this pins the
   * remaining one an operator actually types. ADDREPLICA's literal default is already false, so
   * this asserts no behaviour change rather than a new one -- it guards the parse, which an
   * implementation treating any present property as true would fail while still passing the unset
   * case.
   */
  @Test
  public void testEnvFallbackFalseExplicitlySetSkipsWait() throws Exception {
    String collection = "envfallbackexplicitfalse" + runId.incrementAndGet();
    SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1).process(client);
    cluster.waitForActiveCollection(collection, 1, 1);

    System.setProperty(CommonAdminParams.WAIT_FOR_FINAL_STATE_DEFAULT_PROP, "false");
    TestInjection.prepRecoveryOpPauseForever = "true:100";

    addReplicaWithTimeout(collection, "shard1", 5).process(client);

    DocCollection coll = cluster.getSolrClient().getClusterState().getCollection(collection);
    assertTrue(
        "expected the new replica to still be recovering, since an explicit "
            + CommonAdminParams.WAIT_FOR_FINAL_STATE_DEFAULT_PROP
            + "=false must skip the wait exactly as the unset default does; replicas were "
            + coll.replicaStream().map(r -> r.getName() + "=" + r.getState()).toList(),
        coll.replicaStream().anyMatch(r -> r.getState() != Replica.State.ACTIVE));
  }

  /**
   * The counterpart of the skip-the-wait cases: when the property resolves the flag to true, the
   * same stuck recovery must stop the request from succeeding.
   *
   * <p>The bound observed here is the <em>client's</em> idle timeout, not the request's {@code
   * timeout=5}: that v1 param does not reach the command, so {@link
   * org.apache.solr.cloud.api.collections.AddReplicaCmd}'s own {@code latch.await(timeout,
   * SECONDS)} runs with its 10-minute default and never fires. Hence the dedicated short-timeout
   * client -- the cluster client is built with 90s idle for harsh CI environments, which this test
   * has no reason to spend.
   */
  @Test
  public void testEnvFallbackTrueWaitsWhenRecoveryIsStuck() throws Exception {
    String collection = "envfallbacktrue" + runId.incrementAndGet();
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);

    System.setProperty(CommonAdminParams.WAIT_FOR_FINAL_STATE_DEFAULT_PROP, "true");
    TestInjection.prepRecoveryOpPauseForever = "true:100";

    Exception failure = null;
    try (SolrClient shortIdle =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
            .withConnectionTimeout(5, TimeUnit.SECONDS)
            .withIdleTimeout(15, TimeUnit.SECONDS)
            .build()) {
      try {
        addReplicaWithTimeout(collection, "shard1", 5).process(shortIdle);
      } catch (Exception e) {
        failure = e;
      }
    }

    assertNotNull(
        "expected ADDREPLICA to wait for final state, and so not to return while recovery is "
            + "permanently stuck, but it succeeded",
        failure);
    // pins WHY it failed: a validation or routing error would satisfy assertNotNull alone
    assertTrue(
        "expected the failure to be a timeout, got: " + failure,
        failure.toString().toLowerCase(Locale.ROOT).contains("timeout"));

    DocCollection coll = cluster.getSolrClient().getClusterState().getCollection(collection);
    assertTrue(
        "expected the replica to still be recovering; replicas were "
            + coll.replicaStream().map(r -> r.getName() + "=" + r.getState()).toList(),
        coll.replicaStream().anyMatch(r -> r.getState() != Replica.State.ACTIVE));
  }
}
