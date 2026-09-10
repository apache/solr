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
package org.apache.solr.crossdc.manager.consumer;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that a partition's commit point never runs ahead of work that is still in flight: a work
 * unit's offset may only be committed once that unit and every unit queued before it are done.
 */
@SuppressWarnings("unchecked")
public class PartitionManagerTest {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  private static final TopicPartition PARTITION = new TopicPartition("topic1", 0);

  @SuppressWarnings("unchecked")
  private final KafkaConsumer<String, MirroredSolrRequest<?>> consumer = mock(KafkaConsumer.class);

  private PartitionManager partitionManager;
  private PartitionManager.PartitionWork work;

  @Before
  public void setUp() {
    partitionManager = new PartitionManager(consumer);
    work = partitionManager.getPartitionWork(PARTITION);
  }

  /** Enqueue a work unit owning a single record at the given offset. */
  private PartitionManager.WorkUnit enqueue(long recordOffset) {
    PartitionManager.WorkUnit workUnit = new PartitionManager.WorkUnit(PARTITION);
    work.assignRecord(workUnit, recordOffset);
    return workUnit;
  }

  @Test
  public void testDrainsAllCompletedUnitsInSingleCommit() throws Throwable {
    PartitionManager.WorkUnit first = enqueue(109);
    PartitionManager.WorkUnit second = enqueue(119);
    PartitionManager.WorkUnit third = enqueue(129);

    // the later units finish first - nothing may be committed while the head is in flight
    CompletableFuture<Void> firstWork = new CompletableFuture<>();
    first.workItems.add(firstWork);
    second.workItems.add(CompletableFuture.completedFuture(null));
    third.workItems.add(CompletableFuture.completedFuture(null));

    partitionManager.checkOffsetsAndUpdate(PARTITION);

    verify(consumer, never()).commitSync(anyMap());
    assertEquals(3, work.partitionQueue.size());

    // once the head completes, all three retire under a single commit of the furthest offset
    firstWork.complete(null);
    partitionManager.checkOffsetsAndUpdate(PARTITION);

    verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(130)));
    verifyNoMoreInteractions(consumer);
    assertEquals(0, work.partitionQueue.size());
  }

  @Test
  public void testStopsAtFirstIncompleteUnit() throws Throwable {
    PartitionManager.WorkUnit first = enqueue(109);
    PartitionManager.WorkUnit second = enqueue(119);
    enqueue(129);

    first.workItems.add(CompletableFuture.completedFuture(null));
    second.workItems.add(new CompletableFuture<>());

    partitionManager.checkOffsetsAndUpdate(PARTITION);

    // only the first unit's records are done, so only its offset may be committed
    verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(110)));
    verifyNoMoreInteractions(consumer);
    assertEquals(2, work.partitionQueue.size());
    assertSame(second, work.partitionQueue.peek());
  }

  @Test
  public void testAssignRecordThrowsOnOutOfOrderOffset() {
    PartitionManager.WorkUnit unit = new PartitionManager.WorkUnit(PARTITION);
    work.assignRecord(unit, 109);

    try {
      work.assignRecord(unit, 108);
      fail("expected an out-of-order record offset to be rejected");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testFailedWorkItemPropagatesAndBlocksTheCommit() {
    PartitionManager.WorkUnit first = enqueue(109);
    first.workItems.add(CompletableFuture.failedFuture(new IllegalStateException("boom")));

    Throwable thrown = null;
    try {
      partitionManager.checkOffsetsAndUpdate(PARTITION);
    } catch (Throwable e) {
      thrown = e;
      assertEquals(IllegalStateException.class, e.getClass());
      assertEquals("boom", e.getMessage());
    }
    if (thrown == null) {
      fail("expected the work item failure to be rethrown");
    }

    verify(consumer, never()).commitSync(anyMap());
  }

  /**
   * When a later unit's isComplete() fails after an earlier unit already requires a commit, and the
   * commit itself then also fails (e.g. a partition rebalance mid-drain), the original work-item
   * failure must still be the one that propagates - with the commit failure attached as suppressed
   * rather than replacing the original failure.
   */
  @Test
  public void testCommitFailurePropgates() {
    PartitionManager.WorkUnit first = enqueue(109);
    PartitionManager.WorkUnit second = enqueue(119);

    first.workItems.add(CompletableFuture.completedFuture(null));
    second.workItems.add(CompletableFuture.failedFuture(new IllegalStateException("boom")));

    RuntimeException commitFailure = new RuntimeException("commit failed");
    doThrow(commitFailure).when(consumer).commitSync(anyMap());

    Throwable thrown = null;
    try {
      partitionManager.checkOffsetsAndUpdate(PARTITION);
    } catch (Throwable e) {
      thrown = e;
      // the real root cause must still be the one that surfaces
      assertEquals(IllegalStateException.class, thrown.getClass());
      assertEquals("boom", thrown.getMessage());
      // with the secondary commit failure preserved rather than discarded
      assertEquals(1, thrown.getSuppressed().length);
      assertSame(commitFailure, thrown.getSuppressed()[0]);
    }
    if (thrown == null) {
      fail("expected the work item failure to be rethrown");
    }

    // the earlier unit's offset was still attempted, even though the commit itself failed
    verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(110)));
    assertEquals(1, work.partitionQueue.size());
    assertSame(second, work.partitionQueue.peek());
  }

  /**
   * Should return the existing PartitionWork when the partition is already in the partitionWorkMap
   */
  @Test
  public void testPartitionWorkWhenPartitionInMap() {
    TopicPartition partition = new TopicPartition("test-topic", 0);
    PartitionManager.PartitionWork partitionWork = new PartitionManager.PartitionWork(partition);
    partitionManager.partitionWorkMap.put(partition, partitionWork);

    PartitionManager.PartitionWork result = partitionManager.getPartitionWork(partition);

    assertNotNull(result);
    assertEquals(partitionWork, result);
  }

  /** Should create a new PartitionWork when the partition is not in the partitionWorkMap */
  @Test
  public void testPartitionWorkWhenPartitionNotInMap() {
    TopicPartition partition = new TopicPartition("test-topic", 0);

    PartitionManager.PartitionWork partitionWork = partitionManager.getPartitionWork(partition);

    assertNotNull(partitionWork);
    assertTrue(partitionManager.partitionWorkMap.containsKey(partition));
    assertEquals(partitionWork, partitionManager.partitionWorkMap.get(partition));
  }

  /** Should not update the offset when the future for update is not done */
  @Test
  public void testForOffsetUpdatesWhenFutureNotDone() throws Throwable {
    PartitionManager.WorkUnit workUnit = new PartitionManager.WorkUnit(PARTITION);
    Future<?> future = mock(Future.class);
    when(future.isDone()).thenReturn(false);
    workUnit.workItems.add(future);
    work.assignRecord(workUnit, 0);

    partitionManager.checkOffsetsAndUpdate(PARTITION);

    assertEquals(1, work.partitionQueue.size());
    assertTrue(work.partitionQueue.contains(workUnit));
  }

  /** Should update the offset when the future for update is done */
  @Test
  public void testForOffsetUpdatesWhenFutureDone() throws Throwable {
    PartitionManager.WorkUnit workUnit = new PartitionManager.WorkUnit(PARTITION);
    work.assignRecord(workUnit, 0);

    // Use a real Future instead of a mocked one
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("test"));
    try {
      Future<?> future =
          executor.submit(
              () -> {
                // Simulate the task being completed
              });

      workUnit.workItems.add(future);

      // Wait for the Future to completeE
      future.get(10, TimeUnit.SECONDS);

      partitionManager.checkOffsetsAndUpdate(PARTITION);

      // Verify that the consumer.commitSync() method was called with the correct parameters
      verify(consumer, times(1))
          .commitSync(Map.of(PARTITION, new OffsetAndMetadata(workUnit.nextOffset)));

      // Verify that the partitionQueue is empty after processing
      assertTrue(work.partitionQueue.isEmpty());
    } finally {
      executor.shutdown();
    }
  }

  /** Should check for offset updates for all partitions in the partitionWorkMap */
  @Test
  public void testOffsetUpdatesForAllPartitions() throws Throwable {
    // reuse the shared partition for the first partition, and add a second one
    TopicPartition partition2 = new TopicPartition("topic2", 0);
    PartitionManager.PartitionWork work2 = partitionManager.getPartitionWork(partition2);

    PartitionManager.WorkUnit workUnit1 = new PartitionManager.WorkUnit(PARTITION);
    PartitionManager.WorkUnit workUnit2 = new PartitionManager.WorkUnit(partition2);

    work.assignRecord(workUnit1, 0);
    work2.assignRecord(workUnit2, 0);

    // Create mock Futures and add them to the WorkUnits
    Future<?> mockFuture1 = mock(Future.class);
    Future<?> mockFuture2 = mock(Future.class);

    workUnit1.workItems.add(mockFuture1);
    workUnit2.workItems.add(mockFuture2);

    // Set the mock Futures to be done
    when(mockFuture1.isDone()).thenReturn(true);
    when(mockFuture2.isDone()).thenReturn(true);

    // Call the checkOffsetsAndUpdate method
    partitionManager.checkOffsetsAndUpdate();

    // Verify that the futures were checked for completion
    verify(mockFuture1, times(1)).isDone();
    verify(mockFuture2, times(1)).isDone();

    // Verify that the updateOffset method was called for each partition
    verify(consumer, times(1))
        .commitSync(Map.of(PARTITION, new OffsetAndMetadata(workUnit1.nextOffset)));
    verify(consumer, times(1))
        .commitSync(Map.of(partition2, new OffsetAndMetadata(workUnit2.nextOffset)));
  }
}
