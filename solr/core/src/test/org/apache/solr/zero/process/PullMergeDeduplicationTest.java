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
package org.apache.solr.zero.process;

import java.time.Instant;
import org.apache.solr.core.SolrCore;
import org.apache.solr.zero.util.DeduplicatingList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for {@link DeduplicatingList} using {@link CorePuller.CorePullerMerger}. */
public class PullMergeDeduplicationTest extends ZeroStoreSolrCloudTestCase {

  private static final String COLLECTION_NAME_1 = "collection1";
  private static final String COLLECTION_NAME_2 = "collection2";
  private static final String SHARD_NAME_1 = "shard1";
  private static final String CORE_NAME_1 = "collection1_shard1_replica_z1";
  private static SolrCore core1;

  @BeforeClass
  public static void beforeClass() throws Exception {
    setupCluster(1);
    setupZeroCollectionWithShardNames(COLLECTION_NAME_1, 1, SHARD_NAME_1);
    setupZeroCollectionWithShardNames(COLLECTION_NAME_2, 1, SHARD_NAME_1);
    core1 = cluster.getJettySolrRunner(0).getCoreContainer().getCore(CORE_NAME_1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (core1 != null) core1.close();
  }

  /**
   * Verifies that {@link CorePuller.CorePullerMerger} correctly merges two {@link CorePuller}
   * together
   */
  @Test
  public void testCorePullerMergerMergesSuccessfully() throws Exception {

    CorePuller.CorePullerMerger merger = new CorePuller.CorePullerMerger();

    // verify that merging two of the same CorePuller is no-op and the merge results in exactly
    // the same core pull request
    CorePuller expectedPuller = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 0, 1, Instant.EPOCH);
    CorePuller puller1 = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 0, 1, Instant.EPOCH);
    CorePuller puller2 = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 0, 1, Instant.EPOCH);
    assertCorePuller(puller1, merger.merge(puller1, puller2));

    // verify that merging keeps the larger LastAttemptTimestamp and smaller attempts and queued
    // time
    // expectedPuller should always match the second core pull request argument provided to merge
    expectedPuller = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 3, 1, Instant.ofEpochMilli(1));
    puller1 =
        getTestCorePuller(CORE_NAME_1, Instant.ofEpochMilli(10), 5, 1, Instant.ofEpochMilli(10));
    puller2 = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 3, 2, Instant.ofEpochMilli(1));

    expectedPuller = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 3, 2, Instant.ofEpochMilli(10));
    assertCorePuller(expectedPuller, merger.merge(puller1, puller2));

    shutdownCluster();
  }

  /**
   * Verifies that {@link CorePuller.CorePullerMerger} used in a {@link DeduplicatingList} merges
   * added data successfully
   */
  @Test
  public void testDeduplicatingListWithCorePullerMerger() throws Exception {
    DeduplicatingList<String, CorePuller> dedupList =
        new DeduplicatingList<>(10, new CorePuller.CorePullerMerger());

    // verify that merging two of the same CorePuller is no-op and the merge results in exactly
    // the same pull request
    CorePuller expectedpuller = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 0, 1, Instant.EPOCH);
    CorePuller puller1 = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 0, 2, Instant.EPOCH);
    CorePuller puller2 = getTestCorePuller(CORE_NAME_1, Instant.EPOCH, 0, 3, Instant.EPOCH);
    dedupList.addDeduplicated(puller1, false);
    assertEquals(1, dedupList.size());

    // note the callback here will evaluate if the puller2 matches the expectedpuller;
    // this order is defined in addDeduplicated.
    dedupList.addDeduplicated(puller2, false);
    assertEquals(1, dedupList.size());
    // the 'merged' should just equal the original added core pull request
    assertCorePuller(puller1, dedupList.removeFirst());
  }

  private CorePuller getTestCorePuller(
      String coreName, Instant queuedTime, int attempts, int maxAttempts, Instant lastAttemptTime) {

    return new CorePuller(
        core1, null, null, null, queuedTime, attempts, maxAttempts, lastAttemptTime, null);
  }

  private void assertCorePuller(CorePuller expected, CorePuller actual) {
    assertEquals(
        "CorePuller attempts do not match",
        expected.getCurrentPullAttempt(),
        actual.getCurrentPullAttempt());
    assertEquals(
        "CorePuller lastAttempTimestamps do not match",
        expected.getLastPullAttemptTime(),
        actual.getLastPullAttemptTime());
    assertEquals(
        "CorePuller dedupKeys do not match", expected.getDedupeKey(), actual.getDedupeKey());
    assertEquals(
        "CorePuller queuedTimed do not match", expected.getQueuedTime(), actual.getQueuedTime());
    assertEquals(
        "PullCoreInfos collection names do not match",
        expected.getCollectionName(),
        actual.getCollectionName());
    assertEquals(
        "PullCoreInfos core names do not match", expected.getCoreName(), actual.getCoreName());
    assertEquals(
        "PullCoreInfos shard names do not match", expected.getShardName(), actual.getShardName());
  }
}
