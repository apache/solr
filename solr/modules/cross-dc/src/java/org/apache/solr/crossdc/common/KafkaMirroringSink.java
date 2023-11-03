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
package org.apache.solr.crossdc.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.SLOW_SUBMIT_THRESHOLD_MS;

public class KafkaMirroringSink implements RequestMirroringSink, Closeable {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final KafkaCrossDcConf conf;
    private final Producer<String, MirroredSolrRequest> producer;
    private final KafkaConsumer<String,MirroredSolrRequest> consumer;
    private final String mainTopic;
    private final String dlqTopic;

    public KafkaMirroringSink(final KafkaCrossDcConf conf) {
        // Create Kafka Mirroring Sink
        this.conf = conf;
        this.producer = initProducer();
        this.consumer = initConsumer();
        this.mainTopic = conf.get(KafkaCrossDcConf.TOPIC_NAME).split(",")[0];
        this.dlqTopic = conf.get(KafkaCrossDcConf.DLQ_TOPIC_NAME);

        checkTopicsAvailability();
    }

    @Override
    public void submit(MirroredSolrRequest request) throws MirroringException {
        this.submitRequest(request, mainTopic);
    }

    @Override
    public void submitToDlq(MirroredSolrRequest request) throws MirroringException {
        if (dlqTopic != null) {
            this.submitRequest(request, dlqTopic);
        } else {
            if (log.isInfoEnabled()) {
                log.info("- no DLQ, dropping failed {}", request);
            }
        }
    }

    private void checkTopicsAvailability() {
        final Map<String, List<PartitionInfo>> topics = this.consumer.listTopics();

        if (mainTopic != null && !topics.containsKey(mainTopic)) {
            throw new RuntimeException("Main topic " + mainTopic + " is not available");
        }
        if (dlqTopic != null && !topics.containsKey(dlqTopic)) {
            throw new RuntimeException("DLQ topic " + dlqTopic + " is not available");
        }
    }

    private void submitRequest(MirroredSolrRequest request, String topicName) throws MirroringException {
        if (log.isDebugEnabled()) {
            log.debug("About to submit a MirroredSolrRequest");
        }

        final long enqueueStartNanos = System.nanoTime();

        // Create Producer record
        try {

            producer.send(new ProducerRecord<>(topicName, request), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed adding update to CrossDC queue! request=" + request.getSolrRequest(), exception);
                }
            });

            long lastSuccessfulEnqueueNanos = System.nanoTime();
            // Record time since last successful enqueue as 0
            long elapsedTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - enqueueStartNanos);
            // Update elapsed time

            if (elapsedTimeMillis > conf.getInt(SLOW_SUBMIT_THRESHOLD_MS)) {
                slowSubmitAction(request, elapsedTimeMillis);
            }
        } catch (Exception e) {
            // We are intentionally catching all exceptions, the expected exception form this function is {@link MirroringException}
            String message = "Unable to enqueue request " + request + ", configured retries is" + conf.getInt(KafkaCrossDcConf.NUM_RETRIES) +
                    " and configured max delivery timeout in ms is " + conf.getInt(KafkaCrossDcConf.DELIVERY_TIMEOUT_MS);
            log.error(message, e);
            throw new MirroringException(message, e);
        }
    }

    /**
     * Create and init the producer using {@link this#conf}
     * All producer configs are listed here
     * https://kafka.apache.org/documentation/#producerconfigs
     *
     * @return
     */
    private Producer<String, MirroredSolrRequest> initProducer() {
        // Initialize and return Kafka producer
        Properties kafkaProducerProps = new Properties();

        log.info("Starting CrossDC Producer {}", conf);

        kafkaProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));

        kafkaProducerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        String retries = conf.get(KafkaCrossDcConf.NUM_RETRIES);
        if (retries != null) {
            kafkaProducerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.parseInt(retries));
        }
        kafkaProducerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, conf.getInt(KafkaCrossDcConf.RETRY_BACKOFF_MS));
        kafkaProducerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.DELIVERY_TIMEOUT_MS));
        kafkaProducerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_REQUEST_SIZE_BYTES));
        kafkaProducerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, conf.getInt(KafkaCrossDcConf.BATCH_SIZE_BYTES));
        kafkaProducerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, conf.getInt(KafkaCrossDcConf.BUFFER_MEMORY_BYTES));
        kafkaProducerProps.put(ProducerConfig.LINGER_MS_CONFIG, conf.getInt(KafkaCrossDcConf.LINGER_MS));
        kafkaProducerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.REQUEST_TIMEOUT_MS)); // should be less than time that causes consumer to be kicked out
        kafkaProducerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, conf.get(KafkaCrossDcConf.ENABLE_DATA_COMPRESSION));

        kafkaProducerProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProducerProps.put("value.serializer", MirroredSolrRequestSerializer.class.getName());

        KafkaCrossDcConf.addSecurityProps(conf, kafkaProducerProps);

        kafkaProducerProps.putAll(conf.getAdditionalProperties());

        if (log.isDebugEnabled()) {
            log.debug("Kafka Producer props={}", kafkaProducerProps);
        }

        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        Producer<String, MirroredSolrRequest> producer;
        try {
            producer = new KafkaProducer<>(kafkaProducerProps);
        } finally {
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
        return producer;
    }

    private KafkaConsumer<String, MirroredSolrRequest> initConsumer() {
        final Properties kafkaConsumerProperties = new Properties();

        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, conf.get(KafkaCrossDcConf.GROUP_ID));
        kafkaConsumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_POLL_RECORDS));
        kafkaConsumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, conf.get(KafkaCrossDcConf.MAX_POLL_INTERVAL_MS));
        kafkaConsumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, conf.get(KafkaCrossDcConf.SESSION_TIMEOUT_MS));
        kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConsumerProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MIN_BYTES));
        kafkaConsumerProperties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_WAIT_MS));
        kafkaConsumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_BYTES));
        kafkaConsumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_PARTITION_FETCH_BYTES));
        kafkaConsumerProperties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.REQUEST_TIMEOUT_MS));
        kafkaConsumerProperties.putAll(conf.getAdditionalProperties());

        return new KafkaConsumer<>(kafkaConsumerProperties, new StringDeserializer(), new MirroredSolrRequestSerializer());
    }

    private void slowSubmitAction(Object request, long elapsedTimeMillis) {
        log.warn("Enqueuing the request to Kafka took more than {} millis. enqueueElapsedTime={}",
                conf.get(KafkaCrossDcConf.SLOW_SUBMIT_THRESHOLD_MS),
                elapsedTimeMillis);
    }

    @Override public void close() throws IOException {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
