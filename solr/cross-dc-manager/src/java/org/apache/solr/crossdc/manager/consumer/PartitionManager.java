package org.apache.solr.crossdc.manager.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.*;


public class PartitionManager {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    final ConcurrentHashMap<TopicPartition, PartitionWork> partitionWorkMap = new ConcurrentHashMap<>();
    private final KafkaConsumer<String, MirroredSolrRequest> consumer;


    static class PartitionWork {
        final Queue<WorkUnit> partitionQueue = new LinkedList<>();
    }

    static class WorkUnit {
        final TopicPartition partition;
        Set<Future<?>> workItems = new HashSet<>();
        long nextOffset;

        public WorkUnit(TopicPartition partition) {
            this.partition = partition;
        }
    }


    PartitionManager(KafkaConsumer<String, MirroredSolrRequest> consumer) {
        this.consumer = consumer;
    }

    public PartitionWork getPartitionWork(TopicPartition partition) {
        return partitionWorkMap.compute(partition, (k, v) -> {
            if (v == null) {
                return new PartitionWork();
            }
            return v;
        });
    }


    public void checkOffsetUpdates() throws Throwable {
        for (TopicPartition partition : partitionWorkMap.keySet()) {
            checkForOffsetUpdates(partition);
        }
    }

    void checkForOffsetUpdates(TopicPartition partition) throws Throwable {
        synchronized (partition) {
            PartitionWork work;
            if ((work = partitionWorkMap.get(partition)) != null) {
                WorkUnit workUnit = work.partitionQueue.peek();
                if (workUnit != null) {
                    boolean allFuturesDone = true;
                    for (Future<?> future : workUnit.workItems) {
                        if (!future.isDone()) {
                            if (log.isTraceEnabled()) {
                                log.trace("Future for update is not done topic={}", partition.topic());
                            }
                            allFuturesDone = false;
                            break;
                        }

                        try {
                            future.get();
                        } catch (InterruptedException e) {
                            log.error("Error updating offset for partition: {}", partition, e);
                            throw e;
                        } catch (ExecutionException e) {
                            log.error("Error updating offset for partition: {}", partition, e);
                            throw e.getCause();
                        }

                        if (log.isTraceEnabled()) {
                            log.trace("Future for update is done topic={}", partition.topic());
                        }
                    }

                    if (allFuturesDone) {
                        work.partitionQueue.poll();
                        updateOffset(partition, workUnit.nextOffset);
                    }
                }
            }
        }
    }

    /**
     * Reset the local offset so that the consumer reads the records from Kafka again.
     *
     * @param partition        The TopicPartition to reset the offset for
     * @param partitionRecords PartitionRecords for the specified partition
     */
    private void resetOffsetForPartition(TopicPartition
                                                 partition, List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords) {
        if (log.isTraceEnabled()) {
            log.trace("Resetting offset to: {}", partitionRecords.get(0).offset());
        }
        long resetOffset = partitionRecords.get(0).offset();
        consumer.seek(partition, resetOffset);
    }

    /**
     * Logs and updates the commit point for the partition that has been processed.
     *
     * @param partition  The TopicPartition to update the offset for
     * @param nextOffset The next offset to commit for this partition.
     */
    private void updateOffset(TopicPartition partition, long nextOffset) {
        if (log.isTraceEnabled()) {
            log.trace("Updated offset for topic={} partition={} to offset={}", partition.topic(), partition.partition(), nextOffset);
        }

        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(nextOffset)));
    }

    static long getOffsetForPartition(List<ConsumerRecord<String, MirroredSolrRequest>> partitionRecords) {
        return partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
    }
}
