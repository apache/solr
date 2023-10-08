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

package org.apache.solr.client.solrj.impl;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.processor.TrackingUpdateProcessorFactory;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the behavior of {@link CloudSolrClient#isUpdatesToLeaders} and {@link
 * IsUpdateRequest#isSendToLeaders}.
 *
 * <p>This class uses {@link TrackingUpdateProcessorFactory} instances (configured both before, and
 * after the <code>distrib</code> processor) to inspect which replicas receive various {@link
 * UpdateRequest}s from variously configured {@link CloudSolrClient}s. In some requests, <code>
 * shards.preference=replica.type:PULL</code> is specified to confirm that typical routing
 * prefrences are respected (when the effective value of <code>isSendToLeaders</code> is <code>false
 * </code>)
 */
public class SendUpdatesToLeadersOverrideTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String CONFIG = "tracking-updates";
  private static final String COLLECTION_NAME = "the_collection";

  private static final Set<String> LEADER_CORE_NAMES = new HashSet<>();
  private static final Set<String> PULL_REPLICA_CORE_NAMES = new HashSet<>();

  @AfterClass
  public static void cleanupExpectedCoreNames() throws Exception {
    LEADER_CORE_NAMES.clear();
    PULL_REPLICA_CORE_NAMES.clear();
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    assert LEADER_CORE_NAMES.isEmpty();
    assert PULL_REPLICA_CORE_NAMES.isEmpty();

    final int numNodes = 4;
    configureCluster(numNodes)
        .addConfig(
            CONFIG,
            getFile("solrj")
                .toPath()
                .resolve("solr")
                .resolve("configsets")
                .resolve(CONFIG)
                .resolve("conf"))
        .configure();

    // create 2 shard collection with 1 NRT (leader) and 1 PULL replica
    assertTrue(
        CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG, 2, 1)
            .setPullReplicas(1)
            .setNrtReplicas(1)
            .process(cluster.getSolrClient())
            .isSuccess());

    final List<Replica> allReplicas =
        cluster.getSolrClient().getClusterState().getCollection(COLLECTION_NAME).getReplicas();
    assertEquals(
        "test preconditions were broken, each replica should have it's own node",
        numNodes,
        allReplicas.size());

    allReplicas.stream()
        .filter(Replica::isLeader)
        .map(Replica::getCoreName)
        .collect(Collectors.toCollection(() -> LEADER_CORE_NAMES));

    allReplicas.stream()
        .filter(r -> Replica.Type.PULL.equals(r.getType()))
        .map(Replica::getCoreName)
        .collect(Collectors.toCollection(() -> PULL_REPLICA_CORE_NAMES));

    log.info("Leader coreNames={}", LEADER_CORE_NAMES);
    log.info("PULL Replica coreNames={}", PULL_REPLICA_CORE_NAMES);
  }

  /**
   * Helper that stops recording and returns an unmodifiable list of the core names from each
   * recorded command
   */
  private static List<String> stopRecording(final String group) {
    return TrackingUpdateProcessorFactory.stopRecording(group).stream()
        .map(
            uc ->
                uc.getReq()
                    .getContext()
                    .get(TrackingUpdateProcessorFactory.REQUEST_NODE)
                    .toString())
        .collect(Collectors.toUnmodifiableList());
  }

  /** Convinience class for making assertions about the updates that were processed */
  private static class RecordingResults {
    public final List<UpdateCommand> preDistribCommands;
    public final List<UpdateCommand> postDistribCommands;

    public final Map<SolrQueryRequest, List<UpdateCommand>> preDistribRequests;
    public final Map<SolrQueryRequest, List<UpdateCommand>> postDistribRequests;

    public final Map<String, List<SolrQueryRequest>> preDistribCores;
    public final Map<String, List<SolrQueryRequest>> postDistribCores;

    private static Map<SolrQueryRequest, List<UpdateCommand>> mapReqsToCommands(
        final List<UpdateCommand> commands) {
      return commands.stream().collect(Collectors.groupingBy(UpdateCommand::getReq));
    }

    private static Map<String, List<SolrQueryRequest>> mapCoresToReqs(
        final Collection<SolrQueryRequest> reqs) {
      return reqs.stream()
          .collect(
              Collectors.groupingBy(
                  r -> r.getContext().get(TrackingUpdateProcessorFactory.REQUEST_NODE).toString()));
    }

    public RecordingResults(
        final List<UpdateCommand> preDistribCommands,
        final List<UpdateCommand> postDistribCommands) {
      this.preDistribCommands = preDistribCommands;
      this.postDistribCommands = postDistribCommands;

      this.preDistribRequests = mapReqsToCommands(preDistribCommands);
      this.postDistribRequests = mapReqsToCommands(postDistribCommands);

      this.preDistribCores = mapCoresToReqs(preDistribRequests.keySet());
      this.postDistribCores = mapCoresToReqs(postDistribRequests.keySet());
    }
  }

  /**
   * Given an {@link AbstractUpdateRequest} and a {@link SolrClient}, processes that request against
   * that client while {@link TrackingUpdateProcessorFactory} is recording, does some basic
   * validation, then passes the recorded <code>pre-distrib</code> and <code>post-distrib</code>
   * coreNames to the specified validators
   */
  private static RecordingResults assertUpdateWithRecording(
      final AbstractUpdateRequest req, final SolrClient client) throws Exception {

    TrackingUpdateProcessorFactory.startRecording("pre-distrib");
    TrackingUpdateProcessorFactory.startRecording("post-distrib");

    assertEquals(0, req.process(client, COLLECTION_NAME).getStatus());

    final RecordingResults results =
        new RecordingResults(
            TrackingUpdateProcessorFactory.stopRecording("pre-distrib"),
            TrackingUpdateProcessorFactory.stopRecording("post-distrib"));

    // post-distrib should never match any PULL replicas, regardless of request, if this fails
    // something is seriously wrong with our cluster
    MatcherAssert.assertThat(
        "post-distrib should never be PULL replica",
        results.postDistribCores.keySet(),
        everyItem(not(isIn(PULL_REPLICA_CORE_NAMES))));

    return results;
  }

  /**
   * Since {@link AbstractUpdateRequest#setParam} isn't a fluent API, this is a wrapper helper for
   * setting <code>shards.preference=replica.type:PULL</code> on the input req, and then returning
   * that req
   */
  private static AbstractUpdateRequest prefPull(final AbstractUpdateRequest req) {
    req.setParam("shards.preference", "replica.type:PULL");
    return req;
  }

  public void testBuilderImplicitBehavior() throws Exception {
    try (CloudSolrClient client =
        new CloudLegacySolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .build()) {
      assertTrue(client.isUpdatesToLeaders());
    }
    try (CloudSolrClient client =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .build()) {
      assertTrue(client.isUpdatesToLeaders());
    }
  }

  public void testLegacyClientThatDefaultsToLeaders() throws Exception {
    try (CloudSolrClient client =
        new CloudLegacySolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .sendUpdatesOnlyToShardLeaders()
            .build()) {
      checkUpdatesDefaultToLeaders(client);
      checkUpdatesWithSendToLeadersFalse(client);
    }
  }

  public void testLegacyClientThatDoesNotDefaultToLeaders() throws Exception {
    try (CloudSolrClient client =
        new CloudLegacySolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .sendUpdatesToAnyReplica()
            .build()) {
      checkUpdatesWithShardsPrefPull(client);
      checkUpdatesWithSendToLeadersFalse(client);
    }
  }

  public void testHttp2ClientThatDefaultsToLeaders() throws Exception {
    try (CloudSolrClient client =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .sendUpdatesOnlyToShardLeaders()
            .build()) {
      checkUpdatesDefaultToLeaders(client);
      checkUpdatesWithSendToLeadersFalse(client);
    }
  }

  public void testHttp2ClientThatDoesNotDefaultToLeaders() throws Exception {
    try (CloudSolrClient client =
        new CloudHttp2SolrClient.Builder(
                Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty())
            .sendUpdatesToAnyReplica()
            .build()) {
      checkUpdatesWithShardsPrefPull(client);
      checkUpdatesWithSendToLeadersFalse(client);
    }
  }

  /**
   * Given a SolrClient, sends various updates and asserts expecations regarding default behavior:
   * that these requests will be initially sent to shard leaders, and "routed" requests will be sent
   * to the leader for that route's shard
   */
  private void checkUpdatesDefaultToLeaders(final CloudSolrClient client) throws Exception {
    assertTrue(
        "broken test, only valid on clients where updatesToLeaders=true",
        client.isUpdatesToLeaders());

    { // single doc add is routable and should go to a single shard
      final RecordingResults add =
          assertUpdateWithRecording(new UpdateRequest().add(sdoc("id", "hoss")), client);

      // single NRT leader is only core that should be involved at all
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribCommands, hasSize(1));
      MatcherAssert.assertThat(
          "add pre-distrib must be leader",
          add.preDistribCores.keySet(),
          everyItem(isIn(LEADER_CORE_NAMES)));
      assertEquals(
          "add pre and post should match",
          add.preDistribCores.keySet(),
          add.postDistribCores.keySet());
      assertEquals(
          "add pre and post should be exact same reqs",
          add.preDistribRequests.keySet(),
          add.postDistribRequests.keySet());
      // NOTE: we can't assert the pre/post commands are the same, because they add versioning

      // whatever leader our add was routed to, a DBI for the same id should go to the same leader
      final RecordingResults del =
          assertUpdateWithRecording(new UpdateRequest().deleteById("hoss"), client);
      assertEquals(
          "del pre and post should match",
          del.preDistribCores.keySet(),
          del.postDistribCores.keySet());
      assertEquals(
          "add and del should have been routed the same",
          add.preDistribCores.keySet(),
          del.preDistribCores.keySet());
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribCommands, hasSize(1));
    }

    { // DBQ should start on some leader, and then distrib to both leaders
      final RecordingResults record =
          assertUpdateWithRecording(new UpdateRequest().deleteByQuery("*:*"), client);

      MatcherAssert.assertThat("dbq pre-distrib size", record.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "dbq pre-distrib must be leader",
          record.preDistribCores.keySet(),
          everyItem(isIn(LEADER_CORE_NAMES)));
      MatcherAssert.assertThat(
          "dbq pre-distrib size", record.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("dbq pre-distrib size", record.preDistribCommands, hasSize(1));

      assertEquals(
          "dbq post-distrib must be all leaders",
          LEADER_CORE_NAMES,
          record.postDistribCores.keySet());
      MatcherAssert.assertThat(
          "dbq post-distrib size",
          record.postDistribRequests.keySet(),
          hasSize(LEADER_CORE_NAMES.size()));
      MatcherAssert.assertThat(
          "dbq post-distrib size", record.postDistribCommands, hasSize(LEADER_CORE_NAMES.size()));
    }

    { // When we have multiple direct updates for different shards, client will
      // split them and merge the responses.
      //
      // But we should still only see at most one pre request per shard leader

      final RecordingResults record =
          assertUpdateWithRecording(prefPull(createMultiDirectUpdates(100, 10)), client);

      // NOTE: Don't assume our docIds are spread across multi-shards...
      // ...but the original number of requests should all be diff leaders
      MatcherAssert.assertThat(
          "multi pre-distrib must be leaders",
          record.preDistribCores.keySet(),
          everyItem(isIn(LEADER_CORE_NAMES)));
      MatcherAssert.assertThat(
          "multi pre-distrib req != pre-distrib num cores",
          record.preDistribRequests.keySet(),
          hasSize(record.preDistribCores.keySet().size()));
      MatcherAssert.assertThat(
          "multi pre-distrib command size", record.preDistribCommands, hasSize(100 + 10));

      assertEquals(
          "multi post-distrib must be same leaders",
          record.preDistribCores.keySet(),
          record.postDistribCores.keySet());

      // NOTE: we make no asertion about number of post-distrb requests, just commands
      // (distrib proc may batch differently then what we send)
      assertEquals(
          "multi post-distrib cores don't match pre-distrib cores",
          record.preDistribCores.keySet(),
          record.postDistribCores.keySet());
      MatcherAssert.assertThat(
          "multi post-distrib command size", record.postDistribCommands, hasSize(100 + 10));
    }
  }

  /**
   * Given a SolrClient, sends various updates using {@link #prefPull} and asserts expecations that
   * these requests will be initially sent to PULL replcias
   */
  private void checkUpdatesWithShardsPrefPull(final CloudSolrClient client) throws Exception {

    assertFalse(
        "broken test, only valid on clients where updatesToLeaders=false",
        client.isUpdatesToLeaders());

    { // single doc add...
      final RecordingResults add =
          assertUpdateWithRecording(prefPull(new UpdateRequest().add(sdoc("id", "hoss"))), client);

      // ...should start on (some) PULL replica, since we asked nicely
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "add pre-distrib must be PULL",
          add.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribCommands, hasSize(1));

      // ...then be routed to single leader for this id
      MatcherAssert.assertThat("add post-distrib size", add.postDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "add post-distrib must be leader",
          add.postDistribCores.keySet(),
          everyItem(isIn(LEADER_CORE_NAMES)));
      MatcherAssert.assertThat(
          "add post-distrib size", add.postDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("add post-distrib size", add.postDistribCommands, hasSize(1));

      // A DBI should also start on (some) PULL replica,  since we asked nicely.
      //
      // then it should be distributed to whatever leader our add doc (for the same id) was sent to
      final RecordingResults del =
          assertUpdateWithRecording(prefPull(new UpdateRequest().deleteById("hoss")), client);
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "del pre-distrib must be PULL",
          del.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribCommands, hasSize(1));

      assertEquals(
          "add and del should have same post-distrib leader",
          add.postDistribCores.keySet(),
          del.postDistribCores.keySet());
      MatcherAssert.assertThat(
          "del post-distrib size", del.postDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("del post-distrib size", del.postDistribCommands, hasSize(1));
    }

    { // DBQ start on (some) PULL replica, since we asked nicely, then be routed to all leaders
      final RecordingResults record =
          assertUpdateWithRecording(prefPull(new UpdateRequest().deleteByQuery("*:*")), client);

      MatcherAssert.assertThat("dbq pre-distrib size", record.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "dbq pre-distrib must be PULL",
          record.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat(
          "dbq pre-distrib size", record.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("dbq pre-distrib size", record.preDistribCommands, hasSize(1));

      assertEquals(
          "dbq post-distrib must be all leaders",
          LEADER_CORE_NAMES,
          record.postDistribCores.keySet());
      MatcherAssert.assertThat(
          "dbq post-distrib size",
          record.postDistribRequests.keySet(),
          hasSize(LEADER_CORE_NAMES.size()));
      MatcherAssert.assertThat(
          "dbq post-distrib size", record.postDistribCommands, hasSize(LEADER_CORE_NAMES.size()));
    }

    { // When we sendToLeaders is disabled, a single UpdateRequest containing multiple adds
      // should still only go to one replica for all the "pre" commands, then be forwarded
      // the respective leaders for the "post" commands

      final RecordingResults record =
          assertUpdateWithRecording(prefPull(createMultiDirectUpdates(100, 10)), client);

      MatcherAssert.assertThat(
          "multi pre-distrib size", record.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "multi pre-distrib must be PULL",
          record.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat(
          "multi pre-distrib req size", record.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "multi pre-distrib command size", record.preDistribCommands, hasSize(100 + 10));

      assertEquals(
          "multi post-distrib must be all leaders",
          LEADER_CORE_NAMES,
          record.postDistribCores.keySet());
      // NOTE: Don't assume our docIds are spread across multi-shards...
      //
      // We make no asertion about number of post-distrb requests
      // (distrib proc may batch differently then what we send)
      MatcherAssert.assertThat(
          "multi post-distrib cores",
          record.postDistribCores.keySet(),
          everyItem(isIn(LEADER_CORE_NAMES)));
      MatcherAssert.assertThat(
          "multi post-distrib command size", record.postDistribCommands, hasSize(100 + 10));
    }
  }

  /**
   * Given a SolrClient, sends various updates were {@link IsUpdateRequest#isSendToLeaders} returns
   * false, and asserts expectations that requess using {@link #prefPull} are all sent to PULL
   * replicas, regardless of how the client is configured.
   */
  private void checkUpdatesWithSendToLeadersFalse(final CloudSolrClient client) throws Exception {
    { // single doc add...
      final RecordingResults add =
          assertUpdateWithRecording(
              prefPull(new UpdateRequest().add(sdoc("id", "hoss"))).setSendToLeaders(false),
              client);

      // ...should start on (some) PULL replica, since we asked nicely
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "add pre-distrib must be PULL",
          add.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("add pre-distrib size", add.preDistribCommands, hasSize(1));

      // ...then be routed to single leader for this id
      MatcherAssert.assertThat("add post-distrib size", add.postDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "add post-distrib must be leader",
          add.postDistribCores.keySet(),
          everyItem(isIn(LEADER_CORE_NAMES)));
      MatcherAssert.assertThat(
          "add post-distrib size", add.postDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("add post-distrib size", add.postDistribCommands, hasSize(1));

      // A DBI should also start on (some) PULL replica,  since we asked nicely.
      //
      // then it should be distributed to whatever leader our add doc (for the same id) was sent to
      final RecordingResults del =
          assertUpdateWithRecording(
              prefPull(new UpdateRequest().deleteById("hoss")).setSendToLeaders(false), client);
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "del pre-distrib must be PULL",
          del.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("del pre-distrib size", del.preDistribCommands, hasSize(1));

      assertEquals(
          "add and del should have same post-distrib leader",
          add.postDistribCores.keySet(),
          del.postDistribCores.keySet());
      MatcherAssert.assertThat(
          "del post-distrib size", del.postDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("del post-distrib size", del.postDistribCommands, hasSize(1));
    }

    { // DBQ start on (some) PULL replica, since we asked nicely, then be routed to all leaders
      final RecordingResults record =
          assertUpdateWithRecording(
              prefPull(new UpdateRequest().deleteByQuery("*:*")).setSendToLeaders(false), client);

      MatcherAssert.assertThat("dbq pre-distrib size", record.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "dbq pre-distrib must be PULL",
          record.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat(
          "dbq pre-distrib size", record.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat("dbq pre-distrib size", record.preDistribCommands, hasSize(1));

      assertEquals(
          "dbq post-distrib must be all leaders",
          LEADER_CORE_NAMES,
          record.postDistribCores.keySet());
      MatcherAssert.assertThat(
          "dbq post-distrib size",
          record.postDistribRequests.keySet(),
          hasSize(LEADER_CORE_NAMES.size()));
      MatcherAssert.assertThat(
          "dbq post-distrib size", record.postDistribCommands, hasSize(LEADER_CORE_NAMES.size()));
    }

    { // When we sendToLeaders is disabled, a single UpdateRequest containing multiple adds
      // should still only go to one replica for all the "pre" commands, then be forwarded
      // the respective leaders for the "post" commands

      final RecordingResults record =
          assertUpdateWithRecording(
              prefPull(createMultiDirectUpdates(100, 10)).setSendToLeaders(false), client);

      MatcherAssert.assertThat(
          "multi pre-distrib size", record.preDistribCores.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "multi pre-distrib must be PULL",
          record.preDistribCores.keySet(),
          everyItem(isIn(PULL_REPLICA_CORE_NAMES)));
      MatcherAssert.assertThat(
          "multi pre-distrib req size", record.preDistribRequests.keySet(), hasSize(1));
      MatcherAssert.assertThat(
          "multi pre-distrib command size", record.preDistribCommands, hasSize(100 + 10));

      assertEquals(
          "multi post-distrib must be all leaders",
          LEADER_CORE_NAMES,
          record.postDistribCores.keySet());
      // NOTE: Don't assume our docIds are spread across multi-shards...
      //
      // We make no asertion about number of post-distrb requests
      // (distrib proc may batch differently then what we send)
      MatcherAssert.assertThat(
          "multi post-distrib cores",
          record.postDistribCores.keySet(),
          everyItem(isIn(LEADER_CORE_NAMES)));
      MatcherAssert.assertThat(
          "multi post-distrib command size", record.postDistribCommands, hasSize(100 + 10));
    }
  }

  private static UpdateRequest createMultiDirectUpdates(final int numAdds, final int numDel) {
    final UpdateRequest req = new UpdateRequest();
    for (int i = 0; i < numAdds; i++) {
      req.add(sdoc("id", "add" + i));
    }
    for (int i = 0; i < numDel; i++) {
      req.deleteById("del" + i);
    }
    return req;
  }
}
