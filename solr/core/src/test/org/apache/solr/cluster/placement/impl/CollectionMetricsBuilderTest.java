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
package org.apache.solr.cluster.placement.impl;

import java.util.Arrays;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cluster.placement.CollectionMetrics;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.cluster.placement.ReplicaMetrics;
import org.apache.solr.cluster.placement.ShardMetrics;
import org.junit.Test;

public class CollectionMetricsBuilderTest extends SolrTestCaseJ4 {

  @Test
  public void testMultipleShardLeaders() {
    CollectionMetricsBuilder.ReplicaMetricsBuilder r1 =
        createReplicaMetricsBuilder(
            "r1", ReplicaMetricImpl.INDEX_SIZE_GB, 1.5 * MetricImpl.GB, true);
    CollectionMetricsBuilder.ReplicaMetricsBuilder r2 =
        createReplicaMetricsBuilder(
            "r2", ReplicaMetricImpl.INDEX_SIZE_GB, 2.5 * MetricImpl.GB, true);

    CollectionMetrics metrics = collectionMetricsFromShardReplicaBuilders("shard1", r1, r2);
    ShardMetrics shardMetrics = metrics.getShardMetrics("shard1").get();

    assertTrue("Shard metrics not found", shardMetrics.getLeaderMetrics().isPresent());
    assertTrue(
        "No metrics were present for leader replica", shardMetrics.getLeaderMetrics().isPresent());
    ReplicaMetrics leaderMetrics = shardMetrics.getLeaderMetrics().get();

    // Both replicas claimed to be shard leader, so either metric value is acceptable, and an
    // exception should not be raised
    Double indexSize = leaderMetrics.getReplicaMetric(ReplicaMetricImpl.INDEX_SIZE_GB).get();
    assertTrue(
        "Metric value " + indexSize + " should have matched one of the replica's values",
        indexSize.equals(1.5) || indexSize.equals(2.5));
  }

  @Test
  public void testNoShardLeader() {
    CollectionMetricsBuilder.ReplicaMetricsBuilder r1 =
        createReplicaMetricsBuilder(
            "r1", ReplicaMetricImpl.INDEX_SIZE_GB, 1.5 * MetricImpl.GB, false);
    CollectionMetricsBuilder.ReplicaMetricsBuilder r2 =
        createReplicaMetricsBuilder(
            "r2", ReplicaMetricImpl.INDEX_SIZE_GB, 2.5 * MetricImpl.GB, false);

    CollectionMetrics metrics = collectionMetricsFromShardReplicaBuilders("shard1", r1, r2);
    assertTrue("Shard metrics not found", metrics.getShardMetrics("shard1").isPresent());
    ShardMetrics shardMetrics = metrics.getShardMetrics("shard1").get();

    assertFalse(
        "No replica was leader, so leader metrics should not be present",
        shardMetrics.getLeaderMetrics().isPresent());
  }

  private <T> CollectionMetricsBuilder.ReplicaMetricsBuilder createReplicaMetricsBuilder(
      final String name, final ReplicaMetric<T> metric, final T value, final boolean leader) {
    CollectionMetricsBuilder.ReplicaMetricsBuilder replicaMetricsBuilder =
        new CollectionMetricsBuilder.ReplicaMetricsBuilder(name);
    replicaMetricsBuilder.addMetric(metric, value);
    replicaMetricsBuilder.setLeader(leader);
    return replicaMetricsBuilder;
  }

  private CollectionMetrics collectionMetricsFromShardReplicaBuilders(
      String shardName, CollectionMetricsBuilder.ReplicaMetricsBuilder... replicaMetrics) {
    CollectionMetricsBuilder.ShardMetricsBuilder shardMetricsBuilder =
        new CollectionMetricsBuilder.ShardMetricsBuilder(shardName);
    Arrays.stream(replicaMetrics)
        .forEach(r -> shardMetricsBuilder.replicaMetricsBuilders.put(r.replicaName, r));

    CollectionMetricsBuilder builder = new CollectionMetricsBuilder();
    builder.shardMetricsBuilders.put(shardName, shardMetricsBuilder);

    return builder.build();
  }
}
