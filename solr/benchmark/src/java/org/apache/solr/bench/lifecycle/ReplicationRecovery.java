/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.solr.bench.lifecycle;

import static org.apache.solr.bench.BaseBenchState.log;
import static org.apache.solr.bench.Docs.docs;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.util.concurrent.TimeUnit;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark to measure shard replication/recovery performance.
 *
 * <p>This benchmark creates a collection with 12 shards on a single node, indexes approximately 1GB
 * of data, then adds replicas on a second node to trigger recovery. It measures the time taken for
 * all replicas to become active.
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Fork(value = 1)
public class ReplicationRecovery {

  @State(Scope.Benchmark)
  public static class BenchState {

    static final String COLLECTION = "replicationTestCollection";

    @Param("12")
    int numShards;

    @Param("100")
    int pollIntervalMs;

    // Number of docs to index. Each doc is ~10KB.
    // Use -p docCount=100000 for ~1GB of data.
    @Param("1000")
    int docCount;

    // Auto commit interval in milliseconds
    @Param("10000")
    int autoCommitMaxTime;

    // Number of threads for parallel indexing (0 = sequential)
    @Param("4")
    int indexThreads;

    // Batch size for indexing (docs per request)
    @Param("1000")
    int batchSize;

    // Replica type for new replicas: NRT, TLOG, or PULL
    // PULL replicas just copy segments (fastest for replication)
    // TLOG replicas replay transaction log
    // NRT replicas do full local indexing
    @Param("NRT")
    String replicaType;

    private final Docs largeDocs;
    private String secondNodeUrl;

    public BenchState() {
      // Create docs with substantial content to generate ~100KB per doc
      // This will help us reach ~1GB with 10k docs
      // Using _s (string) fields instead of _t (text) to avoid analysis overhead
      largeDocs =
          docs()
              .field("id", integers().incrementing())
              // Multiple large string fields to bulk up document size to ~100KB (no analysis)
              .field("text1_ss", strings().basicLatinAlphabet().multi(100).ofLengthBetween(200, 400))
              .field("text2_ss", strings().basicLatinAlphabet().multi(100).ofLengthBetween(200, 400))
              .field("text3_ss", strings().basicLatinAlphabet().multi(100).ofLengthBetween(200, 400))
              .field("text4_ss", strings().basicLatinAlphabet().multi(100).ofLengthBetween(200, 400))
              .field("text5_ss", strings().basicLatinAlphabet().multi(80).ofLengthBetween(150, 300))
              .field("text6_ss", strings().basicLatinAlphabet().multi(80).ofLengthBetween(150, 300))
              .field("text7_ss", strings().basicLatinAlphabet().multi(80).ofLengthBetween(150, 300))
              .field("text8_ss", strings().basicLatinAlphabet().multi(80).ofLengthBetween(150, 300))
              .field("content_ss", strings().basicLatinAlphabet().multi(200).ofLengthBetween(100, 200));
    }

    @Setup(Level.Trial)
    public void doSetup(MiniClusterState.MiniClusterBenchState miniClusterState) throws Exception {
      log("Setting up ReplicationRecovery benchmark...");

      // Set autoCommit.maxTime before starting the cluster
      System.setProperty("autoCommit.maxTime", String.valueOf(autoCommitMaxTime));
      log("Set autoCommit.maxTime to " + autoCommitMaxTime + "ms");

      // Start cluster with 2 nodes
      miniClusterState.startMiniCluster(2);

      // Store the second node URL for later use
      secondNodeUrl = miniClusterState.nodes.get(1);
      log("First node URL: " + miniClusterState.nodes.get(0));
      log("Second node URL: " + secondNodeUrl);

      // Create collection with all shards on the first node only (1 replica each)
      log("Creating collection with " + numShards + " shards on first node...");
      CollectionAdminRequest.Create createRequest =
          CollectionAdminRequest.createCollection(COLLECTION, "conf", numShards, 1);
      // Force all replicas to be created on the first node
      // Node name format is host:port_solr (with underscore, not slash)
      String firstNode =
          miniClusterState.nodes.get(0).replace("http://", "").replace("https://", "").replace("/", "_");
      log("First node name for createNodeSet: " + firstNode);
      createRequest.setCreateNodeSet(firstNode);
      miniClusterState.client.requestWithBaseUrl(
          miniClusterState.nodes.get(0), createRequest, null);

      miniClusterState.cluster.waitForActiveCollection(
          COLLECTION, 30, TimeUnit.SECONDS, numShards, numShards);

      log("Collection created. Indexing " + docCount + " documents with " + indexThreads + " threads, batch size " + batchSize + "...");

      // Index documents
      long indexStart = System.currentTimeMillis();
      if (indexThreads > 0) {
        miniClusterState.indexParallelBatched(COLLECTION, largeDocs, docCount, indexThreads, batchSize);
      } else {
        miniClusterState.index(COLLECTION, largeDocs, docCount, false);
      }
      long indexTime = System.currentTimeMillis() - indexStart;
      log("Indexing completed in " + indexTime + "ms");

      // Wait for autoCommit to ensure all data is committed
      log("Waiting for autoCommit (" + autoCommitMaxTime + "ms + buffer)...");
      Thread.sleep(autoCommitMaxTime + 2000);

      log("Setup complete. Ready to benchmark replication recovery.");
    }
  }

  /**
   * Benchmark that measures the time to replicate all shards to a second node.
   *
   * <p>This adds a replica for each of the 12 shards to the second node and polls the cluster state
   * every 100ms until all replicas are active.
   */
  @Benchmark
  public long replicateShards(
      MiniClusterState.MiniClusterBenchState miniClusterState,
      BenchState state,
      Blackhole blackhole)
      throws Exception {

    long startTime = System.currentTimeMillis();
    int totalReplicas = state.numShards * 2; // Original + new replicas

    // Parse replica type
    Replica.Type type = Replica.Type.valueOf(state.replicaType.toUpperCase());
    log("Starting replication of " + state.numShards + " shards to second node (replica type: " + type + ")...");

    // Get the second node name (without http prefix, with underscore) for the replica placement
    String secondNode = state.secondNodeUrl.replace("http://", "").replace("https://", "").replace("/", "_");

    // Add a replica for each shard to the second node
    for (int i = 1; i <= state.numShards; i++) {
      String shardName = "shard" + i;
      CollectionAdminRequest.AddReplica addReplica =
          CollectionAdminRequest.addReplicaToShard(BenchState.COLLECTION, shardName, type);
      addReplica.setNode(secondNode);
      // Send request asynchronously to allow parallel recovery
      addReplica.setAsyncId("add-replica-" + shardName);
      miniClusterState.client.requestWithBaseUrl(miniClusterState.nodes.get(0), addReplica, null);
    }

    log("All add-replica requests submitted. Polling for recovery completion...");

    // Poll cluster state until all replicas are active
    int pollCount = 0;
    boolean allActive = false;
    long lastLogTime = startTime;

    while (!allActive) {
      Thread.sleep(state.pollIntervalMs);
      pollCount++;

      // Refresh and check cluster state
      miniClusterState.cluster.getZkStateReader().forceUpdateCollection(BenchState.COLLECTION);
      DocCollection collection =
          miniClusterState.cluster.getZkStateReader().getCollection(BenchState.COLLECTION);

      int activeCount = 0;
      int recoveringCount = 0;
      int downCount = 0;

      for (Slice slice : collection.getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          Replica.State replicaState = replica.getState();
          if (replicaState == Replica.State.ACTIVE) {
            activeCount++;
          } else if (replicaState == Replica.State.RECOVERING) {
            recoveringCount++;
          } else {
            downCount++;
          }
        }
      }

      // Log progress every 5 seconds
      long now = System.currentTimeMillis();
      if (now - lastLogTime >= 5000) {
        log(
            String.format(
                "Recovery progress: %d active, %d recovering, %d down (total needed: %d)",
                activeCount, recoveringCount, downCount, totalReplicas));
        lastLogTime = now;
      }

      allActive = (activeCount == totalReplicas);
      blackhole.consume(collection);
    }

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    log(
        String.format(
            "Replication complete! All %d replicas active. Duration: %d ms, Poll count: %d",
            totalReplicas, duration, pollCount));

    return duration;
  }
}

