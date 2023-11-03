package org.apache.solr.crossdc.manager.consumer;

import com.codahale.metrics.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class Util {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void logMetrics(MetricRegistry metricRegistry) {
        log.info("Metrics Registry:");
        for (Map.Entry<String, Gauge> entry : metricRegistry.getGauges().entrySet()) {
            log.info("Gauge {}: {}", entry.getKey(), entry.getValue().getValue());
        }
        for (Map.Entry<String, Counter> entry : metricRegistry.getCounters().entrySet()) {
            log.info("Counter {}: {}", entry.getKey(), entry.getValue().getCount());
        }
        for (Map.Entry<String, Histogram> entry : metricRegistry.getHistograms().entrySet()) {
            log.info("Histogram {}: {}", entry.getKey(), entry.getValue().getSnapshot().toString());
        }
        for (Map.Entry<String, Meter> entry : metricRegistry.getMeters().entrySet()) {
            log.info("Meter {}: {}", entry.getKey(), entry.getValue().getCount());
        }
        for (Map.Entry<String, Timer> entry : metricRegistry.getTimers().entrySet()) {
            log.info("Timer {}: {}", entry.getKey(), entry.getValue().getSnapshot().toString());
        }
    }

    public static void printKafkaInfo(String host, String groupId) {
        // Initialize the Kafka Admin Client
        System.out.println("Kafka Info: " + host);
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Get list of topics
            Set<String> topicNames = adminClient.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
            System.out.println("Live Topics: " + topicNames);

            // Initialize the Kafka Consumer Client to fetch offsets
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MirroredSolrRequestSerializer.class.getName());

            try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                for (String topic : topicNames) {
                    Set<TopicPartition> topicPartitions = consumer.assignment();
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    System.out.println("Topic Partitions: " + topicPartitions.size());
                    for (TopicPartition topicPartition : topicPartitions) {
                        if (topicPartition.topic().equals(topic)) {
                            long endOffset = consumer.position(topicPartition);
                            long committedOffset = consumer.committed(topicPartition).offset();
                            long updatesInQueue = endOffset - committedOffset;

                            offsets.put(topicPartition, new OffsetAndMetadata(endOffset));
                            System.out.println("Topic: " + topic);
                            System.out.println("  Partition: " + topicPartition.partition());
                            System.out.println("  End Offset: " + endOffset);
                            System.out.println("  Committed Offset: " + committedOffset);
                            System.out.println("  Updates in Queue: " + updatesInQueue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
