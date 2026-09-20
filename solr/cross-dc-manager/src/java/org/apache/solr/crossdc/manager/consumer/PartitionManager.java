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

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  final ConcurrentHashMap<TopicPartition, PartitionWork> partitionWorkMap =
      new ConcurrentHashMap<>();
  private final KafkaConsumer<String, MirroredSolrRequest<?>> consumer;

  @VisibleForTesting
  public static class PartitionWork {
    final TopicPartition partition;
    final Queue<WorkUnit> partitionQueue = new ArrayDeque<>();

    PartitionWork(TopicPartition partition) {
      this.partition = partition;
    }

    /**
     * Assign a record to a work unit: enqueue the unit on its first record, and advance its commit
     * point to just past that record. A unit that never receives a record is never enqueued, so it
     * can never commit an offset of its own.
     *
     * <p>Guarded by the same monitor as {@link
     * PartitionManager#checkOffsetsAndUpdate(TopicPartition)}, which is the other place the queue
     * is touched.
     *
     * @param unit the work unit the record belongs to
     * @param recordOffset offset of the record being assigned
     * @throws IllegalStateException if recordOffset regresses behind the last record already
     *     assigned to this unit
     */
    synchronized void assignRecord(WorkUnit unit, long recordOffset) {
      // does this work unit belong to the partition we're interested in?
      if (unit.partition != partition.partition()) {
        throw new IllegalStateException(
            "Work unit for partition "
                + partition.partition()
                + " but record for partition "
                + unit.partition);
      }
      if (recordOffset < unit.nextOffset) {
        throw new IllegalStateException(
            "Out-of-order record offset "
                + recordOffset
                + ", expected an offset greater than or equal to "
                + (unit.nextOffset - 1));
      }
      // if this is a new unit enqueue it first
      if (unit.nextOffset < 0) {
        partitionQueue.add(unit);
      }
      // advance the commit point to just past the record
      unit.nextOffset = recordOffset + 1;
    }
  }

  @VisibleForTesting
  public static class WorkUnit {
    final int partition;
    final String topic;
    final Set<Future<?>> workItems = new HashSet<>();

    /**
     * Exclusive upper bound of the offsets this unit owns, i.e. the offset to commit once all of
     * its work items are done. Negative until the unit is assigned its first record.
     */
    long nextOffset = -1;

    WorkUnit(TopicPartition partition) {
      this.partition = partition.partition();
      this.topic = partition.topic();
    }
  }

  PartitionManager(KafkaConsumer<String, MirroredSolrRequest<?>> consumer) {
    this.consumer = consumer;
  }

  public PartitionWork getPartitionWork(TopicPartition partition) {
    return partitionWorkMap.compute(
        partition,
        (k, v) -> {
          if (v == null) {
            return new PartitionWork(partition);
          }
          return v;
        });
  }

  public void checkOffsetsAndUpdate() throws Throwable {
    for (TopicPartition partition : partitionWorkMap.keySet()) {
      checkOffsetsAndUpdate(partition);
    }
  }

  void checkOffsetsAndUpdate(TopicPartition partition) throws Throwable {
    // can't synchronize on the argument (equal but distinct object for different threads)
    // sync on the PartitionWork instead, which is unique per partition and shared by all threads
    // that work on that partition.
    final PartitionWork partitionWork = partitionWorkMap.get(partition);
    // normally impossible because consumer should always call #getPartitionWork first
    // which creates the instance if it doesn't exist.
    if (partitionWork == null) {
      throw new IllegalStateException(
          "PartitionWork for partition " + partition + " not found, likely programming error.");
    }

    synchronized (partitionWork) {
      // remove every completed work unit at the head of the queue, stopping at the first one
      // that is still in flight - a work unit's offset may only be committed once all of the
      // work units before it have been committed too.
      long committableOffset = -1;
      WorkUnit workUnit;
      Throwable failure = null;
      try {
        while ((workUnit = partitionWork.partitionQueue.peek()) != null) {
          if (!isComplete(workUnit, partition)) {
            break;
          }
          // remove completed unit
          partitionWork.partitionQueue.poll();
          committableOffset = workUnit.nextOffset;
        }
      } catch (Throwable t) {
        failure = t;
        throw t;
      } finally {
        // commit whatever progress was already verified in this drain, even if a later
        // unit's isComplete() threw - otherwise that progress silently gets lost.
        if (committableOffset >= 0) {
          try {
            updateOffset(partition, committableOffset);
          } catch (Throwable commitFailure) {
            if (commitFailure instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
            // don't let a secondary commit failure mask the real work-item failure
            if (failure != null) {
              failure.addSuppressed(commitFailure);
            } else {
              throw commitFailure;
            }
          }
        }
      }
    }
  }

  /** Check whether all the work items of this unit are done, rethrowing any of their failures. */
  private boolean isComplete(WorkUnit workUnit, TopicPartition partition) throws Throwable {
    for (Future<?> future : workUnit.workItems) {
      if (!future.isDone()) {
        if (log.isTraceEnabled()) {
          log.trace("Future for update is not done topic={}", partition.topic());
        }
        return false;
      }

      try {
        // the future is already done, so this returns (or rethrows) without waiting
        future.get();
      } catch (InterruptedException e) {
        log.error("Error updating offset for partition (interrupted): {}", partition, e);
        Thread.currentThread().interrupt();
        throw e;
      } catch (CancellationException e) {
        log.error("Error updating offset for partition (cancelled): {}", partition, e);
        throw e;
      } catch (ExecutionException e) {
        log.error("Error updating offset for partition: {}", partition, e);
        throw e.getCause();
      }

      if (log.isTraceEnabled()) {
        log.trace("Future for update is done topic={}", partition.topic());
      }
    }
    return true;
  }

  /**
   * Logs and updates the commit point for the partition that has been processed.
   *
   * @param partition The TopicPartition to update the offset for
   * @param nextOffset The next offset to commit for this partition.
   */
  private void updateOffset(TopicPartition partition, long nextOffset) {
    if (log.isTraceEnabled()) {
      log.trace(
          "Updated offset for topic={} partition={} to offset={}",
          partition.topic(),
          partition.partition(),
          nextOffset);
    }

    consumer.commitSync(Map.of(partition, new OffsetAndMetadata(nextOffset)));
  }
}
