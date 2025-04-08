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
package org.apache.solr.handler.component;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTimeLimitingShardHandler extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * This ensures stats are collected and reported by ShardHandler to the SlowNodeDetector.
   *
   * <p>For through testing on the SlowNodeDetector, please refer to the corresponding
   * TestSlowNodeDetector
   */
  @Test
  public void testDetectionSlowNodes() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String shard =
          "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      shards.add(shard);
      if (serverIndex == 10 || serverIndex == 11) { // 2 slow nodes
        latenciesByShard.put(shard, 2000L);
      }
    }
    try (TestFixture fixture =
        buildTestFixture("solr-shardhandler-timelimited.xml", latenciesByShard, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();

      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      // no exception, since the slow nodes are not detected yet before
      assertNull(response.getException());
      // execution of this shard request
      assertEquals(Set.of("solr-10:8983", "solr-11:8983"), fixture.slowNodeDetector.getSlowNodes());
      assertEquals(
          0, fixture.factory.cancelledSlowNodeRequests.getCount()); // no cancelled requests yet
    }
  }

  @Test
  public void testDetectionSlowNodesMultipleReplicas() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    final int REPLICA_COUNT = 3; // replica per shard
    Map<String, Long> latenciesByShardUrl = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      List<String> shardUrls = new ArrayList<>(); // urls for this shard
      for (int j = 0; j < REPLICA_COUNT; j++) { // each shard has 3 replicas
        String shardUrl =
            "http://solr-"
                + serverIndex
                + ":8983/solr/coll_shard"
                + (i + 1)
                + "_replica_n"
                + (j * i + 1);
        if (serverIndex == 10 || serverIndex == 11) { // 2 slow nodes
          latenciesByShardUrl.put(shardUrl, 2000L);
        }
        shardUrls.add(shardUrl);
        serverIndex++;
      }
      shards.add(String.join("|", shardUrls));
    }
    try (TestFixture fixture =
        buildTestFixture("solr-shardhandler-timelimited.xml", latenciesByShardUrl, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();

      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertNull(
          response
              .getException()); // no exception, since the slow nodes are not detected yet before
      // execution of this shard request
      assertEquals(Set.of("solr-10:8983", "solr-11:8983"), fixture.slowNodeDetector.getSlowNodes());
      assertEquals(
          0, fixture.factory.cancelledSlowNodeRequests.getCount()); // no cancelled requests yet
    }
  }

  /** This ensures handler execution should not be affected if there are no slow nodes tracked */
  @Test
  public void testExecutionNoSlowNodes() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String shard =
          "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      shards.add(shard);
    }
    try (TestFixture fixture =
        buildTestFixture("solr-shardhandler-timelimited.xml", latenciesByShard, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();

      sreq.actualShards = shards.toArray(new String[0]);
      sreq.params = new ModifiableSolrParams();
      sreq.params.set(ShardParams.SHARDS_TOLERANT, true);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());
      assertNull(response.getException());
      assertTrue(fixture.slowNodeDetector.getSlowNodes().isEmpty());
      assertEquals(0, fixture.factory.cancelledSlowNodeRequests.getCount());
    }
  }

  /** This ensures slow node execution would time out if it has been detected as slow before */
  @Test
  public void testExecutionSlowNodes() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String shard =
          "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      if (serverIndex == 10 || serverIndex == 11) { // 2 slow nodes
        latenciesByShard.put(shard, 5000L);
      }
      shards.add(shard);
    }
    try (TestFixture fixture =
        buildTestFixture("solr-shardhandler-timelimited.xml", latenciesByShard, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();
      fixture.slowNodeDetector.setSlowNodes(
          Set.of("solr-10:8983", "solr-11:8983")); // simulate slow nodes in previous run
      sreq.params = new ModifiableSolrParams();
      sreq.params.set(ShardParams.SHARDS_TOLERANT, true);
      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertTrue(response.getException() instanceof CancellationException);
      List<Throwable> exceptions =
          response.getShardRequest().responses.stream()
              .filter(r -> r.getException() != null)
              .map(ShardResponse::getException)
              .collect(Collectors.toList());
      assertEquals(2 * 8, exceptions.size()); // 2 slow nodes, 8 replicas per node

      assertEquals(Set.of("solr-10:8983", "solr-11:8983"), fixture.slowNodeDetector.getSlowNodes());
      assertEquals(2 * 8, fixture.factory.cancelledSlowNodeRequests.getCount());
    }
  }

  /** This ensures slow node execution would NOT be affected if shards.tolerant is not true */
  @Test
  public void testExecutionSlowNodesNotShardsTolerant() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String shard =
          "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      if (serverIndex == 10 || serverIndex == 11) { // 2 slow nodes
        latenciesByShard.put(shard, 5000L);
      }
      shards.add(shard);
    }
    try (TestFixture fixture =
        buildTestFixture("solr-shardhandler-timelimited.xml", latenciesByShard, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();
      fixture.slowNodeDetector.setSlowNodes(
          Set.of("solr-10:8983", "solr-11:8983")); // simulate slow nodes in previous run
      sreq.params = new ModifiableSolrParams(); // NOT shards.tolerant=true
      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertNull(
          response
              .getException()); // no exception as it does NOT time out without shards.tolerant=true
      assertEquals(
          Set.of("solr-10:8983", "solr-11:8983"),
          fixture.slowNodeDetector.getSlowNodes()); // still detect as slow nodes
      assertEquals(0, fixture.factory.cancelledSlowNodeRequests.getCount());
    }
  }

  /** This ensures slow node execution would NOT time out as it's in dry-run mode */
  @Test
  public void testExecutionSlowNodesDryRun() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Map<String, Long> latenciesByShard = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String shard =
          "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      if (serverIndex == 10 || serverIndex == 11) { // 2 slow nodes
        latenciesByShard.put(shard, 5000L);
      }
      shards.add(shard);
    }
    try (TestFixture fixture =
        buildTestFixture("solr-shardhandler-timelimited-dry-run.xml", latenciesByShard, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();
      fixture.slowNodeDetector.setSlowNodes(
          Set.of("solr-10:8983", "solr-11:8983")); // simulate slow nodes in previous run
      sreq.params = new ModifiableSolrParams();
      sreq.params.set(ShardParams.SHARDS_TOLERANT, true);
      sreq.actualShards = shards.toArray(new String[0]);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertNull(response.getException()); // no timeout due to dry-run

      assertEquals(Set.of("solr-10:8983", "solr-11:8983"), fixture.slowNodeDetector.getSlowNodes());
      assertEquals(
          2 * 8,
          fixture.factory.cancelledDryRunSlowNodeRequests
              .getCount()); // different counter for dry-run
      assertEquals(
          0, fixture.factory.cancelledSlowNodeRequests.getCount()); // different counter for dry-run
    }
  }

  /**
   * This ensures slow node execution would time out properly on replication factor greater than 1
   */
  @Test
  public void testExecutionSlowNodesMultipleReplica() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    final int REPLICA_COUNT = 3; // replica per shard
    Map<String, Long> latenciesByShardUrl = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      List<String> shardUrls = new ArrayList<>(); // urls for this shard
      for (int j = 0; j < REPLICA_COUNT; j++) { // each shard has 3 replicas
        String shardUrl =
            "http://solr-"
                + serverIndex
                + ":8983/solr/coll_shard"
                + (i + 1)
                + "_replica_n"
                + (j * i + 1);
        if (serverIndex == 10 || serverIndex == 11) { // 2 slow nodes
          latenciesByShardUrl.put(shardUrl, 2000L);
        }
        shardUrls.add(shardUrl);
        serverIndex++;
      }
      shards.add(String.join("|", shardUrls));
    }

    try (TestFixture fixture =
        buildTestFixture("solr-shardhandler-timelimited.xml", latenciesByShardUrl, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();
      fixture.slowNodeDetector.setSlowNodes(
          Set.of("solr-10:8983", "solr-11:8983")); // simulate slow nodes in previous run

      sreq.actualShards = shards.toArray(new String[0]);
      sreq.params = new ModifiableSolrParams();
      sreq.params.set(ShardParams.SHARDS_TOLERANT, true);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertTrue(response.getException() instanceof CancellationException);
      List<Throwable> exceptions =
          response.getShardRequest().responses.stream()
              .filter(r -> r.getException() != null)
              .map(ShardResponse::getException)
              .collect(Collectors.toList());
      assertEquals(2 * 8, exceptions.size()); // 2 slow nodes, 8 replicas per node

      assertEquals(Set.of("solr-10:8983", "solr-11:8983"), fixture.slowNodeDetector.getSlowNodes());
      assertEquals(2 * 8, fixture.factory.cancelledSlowNodeRequests.getCount());
    }
  }

  /** Ensures with exception code logic would still flow normally */
  public void testException() throws IOException {
    List<String> shards = new ArrayList<>();
    final int SHARD_COUNT = 512;
    Set<String> exceptionUrls = new HashSet<>();
    Map<String, Long> latenciesByShardUrl = new HashMap<>();
    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String shard =
          "http://solr-" + serverIndex + ":8983/solr/coll_shard" + (i + 1) + "_replica_n" + (i + 1);
      shards.add(shard);
      if (serverIndex == 10
          || serverIndex
              == 11) { // 2 nodes with exception even with high latency they cannot be marked as
        // slow node
        exceptionUrls.add(shard);
        latenciesByShardUrl.put(shard, 2000L);
      }
    }
    try (TestFixture fixture =
        buildTestFixture(
            "solr-shardhandler-timelimited.xml", latenciesByShardUrl, exceptionUrls, 100)) {
      org.apache.solr.handler.component.ShardHandler handler = fixture.factory.getShardHandler();
      org.apache.solr.handler.component.ShardRequest sreq =
          new org.apache.solr.handler.component.ShardRequest();

      sreq.actualShards = shards.toArray(new String[0]);
      sreq.params = new ModifiableSolrParams();
      sreq.params.set(ShardParams.SHARDS_TOLERANT, true);
      for (String shard : shards) {
        handler.submit(sreq, shard, new ModifiableSolrParams());
      }

      org.apache.solr.handler.component.ShardResponse response =
          handler.takeCompletedIncludingErrors();
      assertEquals(SHARD_COUNT, response.getShardRequest().responses.size());

      assertTrue(response.getException() instanceof RuntimeException);
      assertTrue(fixture.slowNodeDetector.getSlowNodes().isEmpty());
      assertEquals(
          0, fixture.factory.cancelledSlowNodeRequests.getCount()); // no cancelled requests
    }
  }

  private static TestFixture buildTestFixture(
      String configFile, Map<String, Long> latenciesByUrl, long defaultLatency) {
    return buildTestFixture(configFile, latenciesByUrl, Collections.emptySet(), defaultLatency);
  }

  private static TestFixture buildTestFixture(
      String configFile,
      Map<String, Long> latenciesByUrl,
      Set<String> exceptionUrl,
      long defaultLatency) {
    final Path home = SolrTestCaseJ4.TEST_PATH();
    CoreContainer cc = CoreContainer.createAndLoad(home, home.resolve(configFile));
    TimeLimitingHttpShardHandlerFactory factory =
        (TimeLimitingHttpShardHandlerFactory) cc.getShardHandlerFactory();
    SlowNodeDetector slowNodeDetector =
        new SlowNodeDetector.Builder().withSlowNodeTtl(-1).withSlowLatencyThreshold(1000).build();
    factory.setSlowNodeDetector(slowNodeDetector);
    Http2SolrClient client = new Http2SolrClient.Builder().build();

    ExecutorService executor =
        ExecutorUtil.newMDCAwareCachedThreadPool(
            TestTimeLimitingShardHandler.class.getSimpleName());

    class RspWithServer extends LBSolrClient.Rsp {
      RspWithServer(String server) {
        super();
        this.server = server;
      }
    }
    factory.loadbalancer =
        new LBHttp2SolrClient(client) {
          @Override
          public CompletableFuture<Rsp> requestAsync(Req req) {
            String selectedShardUrl =
                req.getServers().get(0); // first replica is always picked for this impl
            long latency = latenciesByUrl.getOrDefault(selectedShardUrl, defaultLatency);
            return CompletableFuture.supplyAsync(
                () -> {
                  try {
                    Thread.sleep(latency);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // OK
                  }
                  if (exceptionUrl.contains(selectedShardUrl)) {
                    throw new RuntimeException("Simulated exception");
                  }

                  return new RspWithServer(selectedShardUrl);
                },
                executor);
          }
        };

    return new TestFixture(cc, factory, client, executor, slowNodeDetector);
  }
}

class TestFixture implements Closeable {
  CoreContainer cc;
  TimeLimitingHttpShardHandlerFactory factory;
  Http2SolrClient client;
  ExecutorService executorService;
  SlowNodeDetector slowNodeDetector;

  TestFixture(
      CoreContainer cc,
      TimeLimitingHttpShardHandlerFactory factory,
      Http2SolrClient client,
      ExecutorService executorService,
      SlowNodeDetector slowNodeDetector) {
    this.cc = cc;
    this.factory = factory;
    this.client = client;
    this.executorService = executorService;
    this.slowNodeDetector = slowNodeDetector;
  }

  @Override
  public void close() throws IOException {
    if (factory != null) factory.close();
    if (cc != null) cc.shutdown();
    if (client != null) client.close();
    if (executorService != null) {
      ExecutorUtil.shutdownAndAwaitTermination(executorService);
    }
  }
}
