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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PartitionManagerTest {

    @BeforeClass
    public static void ensureWorkingMockito() {
        assumeWorkingMockito();
    }

    /**
     * Should return the existing PartitionWork when the partition is already in the
     * partitionWorkMap
     */
    @Test
    public void getPartitionWorkWhenPartitionInMap() {
        KafkaConsumer<String, MirroredSolrRequest> consumer = mock(KafkaConsumer.class);
        PartitionManager partitionManager = new PartitionManager(consumer);
        TopicPartition partition = new TopicPartition("test-topic", 0);
        PartitionManager.PartitionWork partitionWork = new PartitionManager.PartitionWork();
        partitionManager.partitionWorkMap.put(partition, partitionWork);

        PartitionManager.PartitionWork result = partitionManager.getPartitionWork(partition);

        assertNotNull(result);
        assertEquals(partitionWork, result);
    }

    /**
     * Should create a new PartitionWork when the partition is not in the partitionWorkMap
     */
    @Test
    public void getPartitionWorkWhenPartitionNotInMap() {
        KafkaConsumer<String, MirroredSolrRequest> consumer = mock(KafkaConsumer.class);
        PartitionManager partitionManager = new PartitionManager(consumer);
        TopicPartition partition = new TopicPartition("test-topic", 0);

        PartitionManager.PartitionWork partitionWork = partitionManager.getPartitionWork(partition);

        assertNotNull(partitionWork);
        assertTrue(partitionManager.partitionWorkMap.containsKey(partition));
        assertEquals(partitionWork, partitionManager.partitionWorkMap.get(partition));
    }

    /**
     * Should not update the offset when the future for update is not done
     */
    @Test
    public void checkForOffsetUpdatesWhenFutureNotDone() throws Throwable {
        KafkaConsumer<String, MirroredSolrRequest> consumer = mock(KafkaConsumer.class);
        PartitionManager partitionManager = new PartitionManager(consumer);
        TopicPartition partition = new TopicPartition("test-topic", 0);
        PartitionManager.PartitionWork partitionWork = partitionManager.getPartitionWork(partition);
        PartitionManager.WorkUnit workUnit = new PartitionManager.WorkUnit(partition);
        Future<?> future = mock(Future.class);
        when(future.isDone()).thenReturn(false);
        workUnit.workItems.add(future);
        partitionWork.partitionQueue.add(workUnit);

        partitionManager.checkForOffsetUpdates(partition);

        assertEquals(1, partitionWork.partitionQueue.size());
        assertTrue(partitionWork.partitionQueue.contains(workUnit));
    }

    /**
     * Should update the offset when the future for update is done
     */
    @Test
    public void checkForOffsetUpdatesWhenFutureDone() throws Throwable {
        KafkaConsumer<String, MirroredSolrRequest> consumer = mock(KafkaConsumer.class);
        PartitionManager partitionManager = new PartitionManager(consumer);
        TopicPartition partition = new TopicPartition("test-topic", 0);

        PartitionManager.PartitionWork partitionWork = partitionManager.getPartitionWork(partition);
        PartitionManager.WorkUnit workUnit = new PartitionManager.WorkUnit(partition);
        partitionWork.partitionQueue.add(workUnit);

        // Use a real Future instead of a mocked one
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> {
            // Simulate the task being completed
        });

        workUnit.workItems.add(future);

        // Wait for the Future to completeE
        future.get(10, TimeUnit.SECONDS);

        partitionManager.checkForOffsetUpdates(partition);

        // Verify that the consumer.commitSync() method was called with the correct parameters
        verify(consumer, times(1))
                .commitSync(
                        Collections.singletonMap(
                                partition, new OffsetAndMetadata(workUnit.nextOffset)));

        // Verify that the partitionQueue is empty after processing
        assertTrue(partitionWork.partitionQueue.isEmpty());

        // Shutdown the executor
        executor.shutdown();
    }

    /**
     * Should check for offset updates for all partitions in the partitionWorkMap
     */
    @Test
    public void checkOffsetUpdatesForAllPartitions() throws Throwable { // Create a mock KafkaConsumer
        KafkaConsumer<String, MirroredSolrRequest> mockConsumer = mock(KafkaConsumer.class);

        // Create a PartitionManager instance with the mock KafkaConsumer
        PartitionManager partitionManager = new PartitionManager(mockConsumer);

        // Create a few TopicPartitions
        TopicPartition partition1 = new TopicPartition("topic1", 0);
        TopicPartition partition2 = new TopicPartition("topic2", 0);

        // Add some PartitionWork to the partitionWorkMap
        PartitionManager.PartitionWork work1 = partitionManager.getPartitionWork(partition1);
        PartitionManager.PartitionWork work2 = partitionManager.getPartitionWork(partition2);

        // Create WorkUnits and add them to the PartitionWork
        PartitionManager.WorkUnit workUnit1 = new PartitionManager.WorkUnit(partition1);
        PartitionManager.WorkUnit workUnit2 = new PartitionManager.WorkUnit(partition2);

        work1.partitionQueue.add(workUnit1);
        work2.partitionQueue.add(workUnit2);

        // Create mock Futures and add them to the WorkUnits
        Future<?> mockFuture1 = mock(Future.class);
        Future<?> mockFuture2 = mock(Future.class);

        workUnit1.workItems.add(mockFuture1);
        workUnit2.workItems.add(mockFuture2);

        // Set the mock Futures to be done
        when(mockFuture1.isDone()).thenReturn(true);
        when(mockFuture2.isDone()).thenReturn(true);

        // Call the checkOffsetUpdates method
        partitionManager.checkOffsetUpdates();

        // Verify that the futures were checked for completion
        verify(mockFuture1, times(1)).isDone();
        verify(mockFuture2, times(1)).isDone();


        // Verify that the updateOffset method was called for each partition
        verify(mockConsumer, times(1))
                .commitSync(
                        Collections.singletonMap(
                                partition1, new OffsetAndMetadata(workUnit1.nextOffset)));
        verify(mockConsumer, times(1))
                .commitSync(
                        Collections.singletonMap(
                                partition2, new OffsetAndMetadata(workUnit2.nextOffset)));
    }
}