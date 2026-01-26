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

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.response.PrometheusResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void logMetrics(SolrMetricManager metricManager) {
    SolrQueryResponse rsp = new SolrQueryResponse();
    new MetricsHandler(metricManager)
        .handleRequest(SolrParams.of(), (key, value) -> rsp.add(key, value));
    String output;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      new PrometheusResponseWriter()
          .write(baos, null, rsp, PrometheusResponseWriter.CONTENT_TYPE_PROMETHEUS);
      output = baos.toString();
    } catch (Exception e) {
      log.error("Error while writing final metrics", e);
      output = rsp.toString();
    }
    log.info("#### Consumer Metrics: ####\n{}", output);
    //    List<MetricSnapshot> snapshotList =
    //        metricManager.getPrometheusMetricReaders().values().stream()
    //            .flatMap(r -> r.collect().stream())
    //            .toList();
    //    MetricSnapshots snapshots = MetricSnapshots.of(snapshotList.toArray(new
    // MetricSnapshot[0]));
    //    String output;
    //    try {
    //      ByteArrayOutputStream baos = new ByteArrayOutputStream();
    //      new PrometheusTextFormatWriter(false).write(baos, snapshots);
    //      output = baos.toString();
    //    } catch (Exception e) {
    //      log.error("Error while writing final metrics", e);
    //      output = snapshots.toString();
    //    }
    //    log.info("#### Consumer Metrics: ####\n{}", output);
  }

  public static void printKafkaInfo(String host, String groupId) {
    // Initialize the Kafka Admin Client
    log.info("Kafka Info: {}", host);
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host);
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      // Get list of topics
      Set<String> topicNames =
          adminClient.listTopics(new ListTopicsOptions().listInternal(false)).names().get();
      log.info("Live Topics: {}", topicNames);

      // Initialize the Kafka Consumer Client to fetch offsets
      Properties consumerProps = new Properties();
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      consumerProps.put(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      consumerProps.put(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          MirroredSolrRequestSerializer.class.getName());

      try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
        for (String topic : topicNames) {
          Set<TopicPartition> topicPartitions = consumer.assignment();
          if (log.isInfoEnabled()) {
            log.info("Topic Partitions: {}", topicPartitions.size());
          }
          for (TopicPartition topicPartition : topicPartitions) {
            if (topicPartition.topic().equals(topic)) {
              long endOffset = consumer.position(topicPartition);
              long committedOffset = consumer.committed(topicPartition).offset();
              long updatesInQueue = endOffset - committedOffset;

              log.info("Topic: {}", topic);
              if (log.isInfoEnabled()) {
                log.info("  Partition: {}", topicPartition.partition());
              }
              log.info("  End Offset: {}", endOffset);
              log.info("  Committed Offset: {}", committedOffset);
              log.info("  Updates in Queue: {}", updatesInQueue);
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error while fetching Kafka info", e);
    }
  }
}
