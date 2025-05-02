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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSlowNodeDetector extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testDetection0SlowNode() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";
      stats.recordLatency(node, 200);
    }

    SlowNodeDetector detector = new SlowNodeDetector.Builder().build();
    detector.notifyRequestStats(stats);
    assertEquals(Collections.emptySet(), detector.getSlowNodes());
  }

  @Test
  public void testDetection1SlowNode() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 1) { // 1 slow nodes
        stats.recordLatency(node, 10000);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector = new SlowNodeDetector.Builder().build();
    detector.notifyRequestStats(stats);
    assertEquals(Set.of("solr-1:8983"), detector.getSlowNodes());
  }

  @Test
  public void testDetection2SlowNodes() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 10 || serverIndex == 15) { // 2 slow nodes
        stats.recordLatency(node, 10000);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector = new SlowNodeDetector.Builder().build();
    detector.notifyRequestStats(stats);
    assertEquals(Set.of("solr-10:8983", "solr-15:8983"), detector.getSlowNodes());
  }

  /** Most nodes are slow except 1. No slow node detected */
  @Test
  public void testDetection1FastNode() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 10 || serverIndex == 15) { // 1 fast node
        stats.recordLatency(node, 2000);
      } else {
        stats.recordLatency(node, 10000);
      }
    }

    SlowNodeDetector detector = new SlowNodeDetector.Builder().build();
    detector.notifyRequestStats(stats);
    assertEquals(Collections.emptySet(), detector.getSlowNodes());
  }

  /** 1 slow node at 10s. But still less than 15s limit. Therefore, no slow node detected */
  @Test
  public void testDetectionNoSlowNodeWithLatencyThreshold() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 1) {
        stats.recordLatency(node, 10000);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector =
        new SlowNodeDetector.Builder().withSlowLatencyThreshold(15000).build();
    detector.notifyRequestStats(stats);
    assertEquals(Collections.emptySet(), detector.getSlowNodes());
  }

  /**
   * 1 slow node at 10s. But dropping from 10 to 2 sec is 0.2, which is greater than 0.1 drop
   * latency ratio limit. Therefore, no slow node detected
   */
  @Test
  public void testDetectionNoSlowNodeWithRatio() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex
          == 1) { // 1 slow node. But dropping from 10 -> 2 sec is 0.2, which is > 0.1 drop latency
        // ratio limit
        stats.recordLatency(node, 10000);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector =
        new SlowNodeDetector.Builder().withLatencyDropRatioThreshold(0.1).build();
    detector.notifyRequestStats(stats);
    assertEquals(Collections.emptySet(), detector.getSlowNodes());
  }

  /**
   * 1 slow node at 10s. But only 512 cores, which is less than 1024 settings. Therefore, no slow
   * node detected
   */
  @Test
  public void testDetectionNoSlowNodeWithMinCorePerRequest() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 1) {
        stats.recordLatency(node, 10000);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector =
        new SlowNodeDetector.Builder().withMinShardCountPerRequest(1024).build();
    detector.notifyRequestStats(stats);
    assertEquals(Collections.emptySet(), detector.getSlowNodes());
  }

  /** 1 slow node at 10s. But iteration threshold at . Therefore, no slow node detected */
  @Test
  public void testDetectionNoSlowNodeWithMaxSlowResponsePercentage() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 1) {
        stats.recordLatency(node, 10000);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector =
        new SlowNodeDetector.Builder().withMaxSlowResponsePercentage(1).build();
    detector.notifyRequestStats(stats);
    assertEquals(Collections.emptySet(), detector.getSlowNodes());
  }

  @Test
  public void testDetection1SlowNode1RecoveredNode() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 1) { // 1 slow nodes
        stats.recordLatency(node, 10000);
      } else if (serverIndex == 2) { // recovered nodes
        stats.recordLatency(node, 1999);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector = new SlowNodeDetector.Builder().build();
    detector.setSlowNodes(Set.of("solr-2:8983"));
    detector.notifyRequestStats(stats);
    assertEquals(Set.of("solr-1:8983"), detector.getSlowNodes()); // solr-2:8983 is recovered
  }

  @Test
  public void testDetection1SlowNode0RecoveredNode() {
    RequestStats stats = new RequestStats();
    final int SHARD_COUNT = 512;

    for (int i = 0; i < SHARD_COUNT; i++) {
      int serverIndex = (i / 8 + 1);
      String node = "solr-" + serverIndex + ":8983";

      if (serverIndex == 1) { // 1 slow nodes
        stats.recordLatency(node, 10000);
      } else if (serverIndex == 2 && i % 2 == 0) {
        // this node is not considered slow (only some shards are slow), however it has not
        // recovered either
        stats.recordLatency(node, 10000);
      } else {
        stats.recordLatency(node, 2000);
      }
    }

    SlowNodeDetector detector = new SlowNodeDetector.Builder().build();
    detector.setSlowNodes(Set.of("solr-2:8983"));
    detector.notifyRequestStats(stats);
    assertEquals(Set.of("solr-1:8983", "solr-2:8983"), detector.getSlowNodes());
  }
}
