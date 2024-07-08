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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.crossdc.common.MirroringException;
import org.apache.solr.crossdc.manager.messageprocessor.SolrMessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Java class called KafkaCrossDcConsumer, which is part of the Apache Solr framework. It
 * consumes messages from Kafka and mirrors them into a Solr instance. It uses a KafkaConsumer
 * object to subscribe to one or more topics and receive ConsumerRecords that contain
 * MirroredSolrRequest objects. The SolrMessageProcessor handles each MirroredSolrRequest and sends
 * the resulting UpdateRequest to the CloudSolrClient for indexing. A ThreadPoolExecutor is used to
 * handle the update requests asynchronously. The KafkaCrossDcConsumer also handles offset
 * management, committing offsets to Kafka and can seek to specific offsets for error recovery. The
 * class provides methods to start and top the consumer thread.
 */
public class KafkaCrossDcConsumer extends Consumer.CrossDcConsumer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final MetricRegistry metrics =
      SharedMetricRegistries.getOrCreate(Consumer.METRICS_REGISTRY);

  private final KafkaConsumer<String, MirroredSolrRequest<?>> kafkaConsumer;
  private final CountDownLatch startLatch;
  KafkaMirroringSink kafkaMirroringSink;

  private static final int KAFKA_CONSUMER_POLL_TIMEOUT_MS = 5000;
  private final String[] topicNames;
  private final int maxAttempts;
  private final CrossDcConf.CollapseUpdates collapseUpdates;
  private final int maxCollapseRecords;
  private final SolrMessageProcessor messageProcessor;

  protected final CloudSolrClient solrClient;

  private final ThreadPoolExecutor executor;

  private final ExecutorService offsetCheckExecutor =
      ExecutorUtil.newMDCAwareCachedThreadPool(r -> new Thread(r, "offset-check-thread"));
  private final PartitionManager partitionManager;

  private final BlockingQueue<Runnable> queue = new BlockingQueue<>(10);

  /**
   * @param conf The Kafka consumer configuration
   * @param startLatch To inform the caller when the Consumer has started
   */
  public KafkaCrossDcConsumer(KafkaCrossDcConf conf, CountDownLatch startLatch) {

    this.topicNames = conf.get(KafkaCrossDcConf.TOPIC_NAME).split(",");
    this.maxAttempts = conf.getInt(KafkaCrossDcConf.MAX_ATTEMPTS);
    this.collapseUpdates =
        CrossDcConf.CollapseUpdates.getOrDefault(
            conf.get(CrossDcConf.COLLAPSE_UPDATES), CrossDcConf.CollapseUpdates.PARTIAL);
    this.maxCollapseRecords = conf.getInt(KafkaCrossDcConf.MAX_COLLAPSE_RECORDS);
    this.startLatch = startLatch;
    final Properties kafkaConsumerProps = new Properties();

    kafkaConsumerProps.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.get(KafkaCrossDcConf.BOOTSTRAP_SERVERS));

    kafkaConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, conf.get(KafkaCrossDcConf.GROUP_ID));

    kafkaConsumerProps.put(
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, conf.getInt(KafkaCrossDcConf.MAX_POLL_RECORDS));

    kafkaConsumerProps.put(
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
        conf.get(KafkaCrossDcConf.MAX_POLL_INTERVAL_MS));

    kafkaConsumerProps.put(
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, conf.get(KafkaCrossDcConf.SESSION_TIMEOUT_MS));
    kafkaConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    kafkaConsumerProps.put(
        ConsumerConfig.FETCH_MIN_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MIN_BYTES));
    kafkaConsumerProps.put(
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_WAIT_MS));

    kafkaConsumerProps.put(
        ConsumerConfig.FETCH_MAX_BYTES_CONFIG, conf.getInt(KafkaCrossDcConf.FETCH_MAX_BYTES));
    kafkaConsumerProps.put(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
        conf.getInt(KafkaCrossDcConf.MAX_PARTITION_FETCH_BYTES));

    kafkaConsumerProps.put(
        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, conf.getInt(KafkaCrossDcConf.REQUEST_TIMEOUT_MS));

    KafkaCrossDcConf.addSecurityProps(conf, kafkaConsumerProps);

    kafkaConsumerProps.putAll(conf.getAdditionalProperties());
    int threads = conf.getInt(KafkaCrossDcConf.CONSUMER_PROCESSING_THREADS);

    executor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            threads,
            threads,
            0L,
            TimeUnit.MILLISECONDS,
            queue,
            r -> new Thread(r, "KafkaCrossDcConsumerWorker"));
    executor.prestartAllCoreThreads();

    solrClient = createSolrClient(conf);

    messageProcessor = createSolrMessageProcessor();

    log.info("Creating Kafka consumer with configuration {}", kafkaConsumerProps);
    kafkaConsumer = createKafkaConsumer(kafkaConsumerProps);
    partitionManager = new PartitionManager(kafkaConsumer);
    // Create producer for resubmitting failed requests
    log.info("Creating Kafka resubmit producer");
    this.kafkaMirroringSink = createKafkaMirroringSink(conf);
    log.info("Created Kafka resubmit producer");
  }

  protected SolrMessageProcessor createSolrMessageProcessor() {
    return new SolrMessageProcessor(solrClient, resubmitRequest -> 0L);
  }

  public KafkaConsumer<String, MirroredSolrRequest<?>> createKafkaConsumer(Properties properties) {
    return new KafkaConsumer<>(
        properties, new StringDeserializer(), new MirroredSolrRequestSerializer());
  }

  /**
   * This is where the magic happens.
   *
   * <ol>
   *   <li>Polls and gets the packets from the queue
   *   <li>Extract the MirroredSolrRequest objects
   *   <li>Send the request to the MirroredSolrRequestHandler that has the processing, retry, error
   *       handling logic.
   * </ol>
   */
  @Override
  public void run() {
    Collection<String> topicNameList = Arrays.asList(topicNames);
    log.info("About to start Kafka consumer thread, topics={}", topicNameList);

    try {

      kafkaConsumer.subscribe(topicNameList);

      log.info("Consumer started");
      startLatch.countDown();

      while (pollAndProcessRequests()) {
        // no-op within this loop: everything is done in pollAndProcessRequests method defined
        // above.
      }

      log.info("Closed kafka consumer. Exiting now.");
      try {
        kafkaConsumer.close();
      } catch (Exception e) {
        log.warn("Failed to close kafka consumer", e);
      }

      try {
        kafkaMirroringSink.close();
      } catch (Exception e) {
        log.warn("Failed to close kafka mirroring sink", e);
      }
    } finally {
      IOUtils.closeQuietly(solrClient);
    }
  }

  /**
   * Polls and processes the requests from Kafka. This method returns false when the consumer needs
   * to be shutdown i.e. when there's a wakeup exception.
   */
  boolean pollAndProcessRequests() {
    log.trace("Entered pollAndProcessRequests loop");
    try {
      try {
        partitionManager.checkOffsetUpdates();
      } catch (Throwable e) {
        log.error("Error while checking offset updates, shutting down", e);
        return false;
      }

      ConsumerRecords<String, MirroredSolrRequest<?>> records =
          kafkaConsumer.poll(Duration.ofMillis(KAFKA_CONSUMER_POLL_TIMEOUT_MS));

      if (log.isTraceEnabled()) {
        log.trace("poll return {} records", records.count());
      }

      UpdateRequest updateReqBatch = null;
      int currentCollapsed = 0;

      ConsumerRecord<String, MirroredSolrRequest<?>> lastRecord = null;

      for (TopicPartition partition : records.partitions()) {
        List<ConsumerRecord<String, MirroredSolrRequest<?>>> partitionRecords =
            records.records(partition);

        PartitionManager.PartitionWork partitionWork = partitionManager.getPartitionWork(partition);
        PartitionManager.WorkUnit workUnit = new PartitionManager.WorkUnit(partition);
        workUnit.nextOffset = PartitionManager.getOffsetForPartition(partitionRecords);
        partitionWork.partitionQueue.add(workUnit);
        try {
          ModifiableSolrParams lastUpdateParams = null;
          NamedList<?> lastUpdateParamsAsNamedList = null;
          for (ConsumerRecord<String, MirroredSolrRequest<?>> requestRecord : partitionRecords) {
            if (log.isTraceEnabled()) {
              log.trace(
                  "Fetched record from topic={} partition={} key={} value={}",
                  requestRecord.topic(),
                  requestRecord.partition(),
                  requestRecord.key(),
                  requestRecord.value());
            }

            lastRecord = requestRecord;
            MirroredSolrRequest<?> req = requestRecord.value();
            SolrRequest<?> solrReq = req.getSolrRequest();
            MirroredSolrRequest.Type type = req.getType();
            metrics.counter(MetricRegistry.name(type.name(), "input")).inc();
            ModifiableSolrParams params = new ModifiableSolrParams(solrReq.getParams());
            if (log.isTraceEnabled()) {
              log.trace("-- picked type={}, params={}", req.getType(), params);
            }

            // determine if it's an UPDATE with deletes, or if the existing batch has deletes
            boolean hasDeletes = false;
            if (type == MirroredSolrRequest.Type.UPDATE) {
              UpdateRequest ureq = (UpdateRequest) solrReq;
              hasDeletes = hasDeletes(ureq) || hasDeletes(updateReqBatch);
            }

            // it's an update but with different params
            if (type == MirroredSolrRequest.Type.UPDATE
                && (
                // different params
                (lastUpdateParams != null
                        && !lastUpdateParams.toNamedList().equals(params.toNamedList()))
                    ||
                    // no collapsing
                    (collapseUpdates == CrossDcConf.CollapseUpdates.NONE)
                    ||
                    // partial collapsing but has deletes
                    (collapseUpdates == CrossDcConf.CollapseUpdates.PARTIAL && hasDeletes)
                    ||
                    // too many collapsed - emit
                    currentCollapsed >= maxCollapseRecords)) {
              if (log.isTraceEnabled()) {
                log.trace("Starting new UpdateRequest, params={}", params);
              }
              // send previous batch, if any
              if (updateReqBatch != null) {
                sendBatch(updateReqBatch, type, lastRecord, workUnit);
              }
              updateReqBatch = null;
              lastUpdateParamsAsNamedList = null;
              currentCollapsed = 0;
              workUnit = new PartitionManager.WorkUnit(partition);
              workUnit.nextOffset = PartitionManager.getOffsetForPartition(partitionRecords);
              partitionWork.partitionQueue.add(workUnit);
            }

            lastUpdateParams = params;
            if (type == MirroredSolrRequest.Type.UPDATE) {
              if (updateReqBatch == null) {
                // just initialize
                updateReqBatch = new UpdateRequest();
              } else {
                if (collapseUpdates == CrossDcConf.CollapseUpdates.NONE) {
                  throw new RuntimeException("Can't collapse requests.");
                }
                if (collapseUpdates == CrossDcConf.CollapseUpdates.PARTIAL && hasDeletes) {
                  throw new RuntimeException("Can't collapse requests with deletions.");
                }
                metrics.counter(MetricRegistry.name(type.name(), "collapsed")).inc();
                currentCollapsed++;
              }
              UpdateRequest update = (UpdateRequest) solrReq;
              MirroredSolrRequest.setParams(updateReqBatch, params);
              if (lastUpdateParamsAsNamedList == null) {
                lastUpdateParamsAsNamedList = lastUpdateParams.toNamedList();
              }
              // merge
              List<SolrInputDocument> docs = update.getDocuments();
              if (docs != null) {
                updateReqBatch.add(docs);
                metrics.counter(MetricRegistry.name(type.name(), "add")).inc(docs.size());
              }
              List<String> deletes = update.getDeleteById();
              if (deletes != null) {
                updateReqBatch.deleteById(deletes);
                metrics.counter(MetricRegistry.name(type.name(), "dbi")).inc(deletes.size());
              }
              List<String> deleteByQuery = update.getDeleteQuery();
              if (deleteByQuery != null) {
                for (String delByQuery : deleteByQuery) {
                  updateReqBatch.deleteByQuery(delByQuery);
                }
                metrics.counter(MetricRegistry.name(type.name(), "dbq")).inc(deleteByQuery.size());
              }
            } else {
              // non-update requests should be sent immediately
              sendBatch(req.getSolrRequest(), type, lastRecord, workUnit);
            }
          }

          if (updateReqBatch != null) {
            sendBatch(updateReqBatch, MirroredSolrRequest.Type.UPDATE, lastRecord, workUnit);
          }
          try {
            partitionManager.checkForOffsetUpdates(partition);
          } catch (Throwable e) {
            log.error("Error while checking offset updates, shutting down", e);
            return false;
          }

          // handleItem sets the thread interrupt, let's exit if there has been an interrupt set
          if (Thread.currentThread().isInterrupted()) {
            log.info("Kafka Consumer thread interrupted, shutting down Kafka consumer.");
            return false;
          }
        } catch (WakeupException e) {
          log.info("Caught wakeup exception, shutting down KafkaSolrRequestConsumer.");
          return false;
        } catch (Exception e) {
          // If there is any exception returned by handleItem, don't set the offset.

          if (e instanceof ClassCastException
              || e instanceof SerializationException) { // TODO: optional
            log.error("Non retryable error", e);
            return false;
          }
          log.error("Exception occurred in Kafka consumer thread, stopping the Consumer.", e);
          return false;
        }
      }

      try {
        partitionManager.checkOffsetUpdates();
      } catch (Throwable e) {
        log.error("Error while checking offset updates, shutting down", e);
        return false;
      }

    } catch (WakeupException e) {
      log.info("Caught wakeup exception, shutting down KafkaSolrRequestConsumer");
      return false;
    } catch (Exception e) {

      if (e instanceof ClassCastException
          || e instanceof SerializationException) { // TODO: optional
        log.error("Non retryable error", e);
        return false;
      }

      log.error("Exception occurred in Kafka consumer thread, but we will continue.", e);
    }
    return true;
  }

  private boolean hasDeletes(UpdateRequest ureq) {
    if (ureq == null) {
      return false;
    }
    return (ureq.getDeleteByIdMap() != null && !ureq.getDeleteByIdMap().isEmpty())
        || (ureq.getDeleteQuery() != null && !ureq.getDeleteQuery().isEmpty());
  }

  public void sendBatch(
      SolrRequest<?> solrReqBatch,
      MirroredSolrRequest.Type type,
      ConsumerRecord<String, MirroredSolrRequest<?>> lastRecord,
      PartitionManager.WorkUnit workUnit) {
    // Kafka client is not thread-safe !!!
    Future<?> future =
        executor.submit(
            () -> {
              try {
                final MirroredSolrRequest<?> mirroredSolrRequest =
                    new MirroredSolrRequest<>(type, lastRecord.value().getAttempt(), solrReqBatch);
                final IQueueHandler.Result<MirroredSolrRequest<?>> result =
                    messageProcessor.handleItem(mirroredSolrRequest);

                processResult(type, result);
              } catch (MirroringException e) {
                // We don't really know what to do here
                log.error(
                    "Mirroring exception occurred while resubmitting to Kafka. We are going to stop the consumer thread now.",
                    e);
                throw new RuntimeException(e);
              }
            });
    workUnit.workItems.add(future);
  }

  protected void processResult(
      MirroredSolrRequest.Type type, IQueueHandler.Result<MirroredSolrRequest<?>> result)
      throws MirroringException {
    MirroredSolrRequest<?> item = result.getItem();
    switch (result.status()) {
      case FAILED_RESUBMIT:
        if (log.isTraceEnabled()) {
          log.trace("result=failed-resubmit");
        }
        final int attempt = item.getAttempt();
        if (attempt > this.maxAttempts) {
          log.info(
              "Sending message to dead letter queue because of max attempts limit with current value = {}",
              attempt);
          kafkaMirroringSink.submitToDlq(item);
          metrics.counter(MetricRegistry.name(type.name(), "failed-dlq")).inc();
        } else {
          kafkaMirroringSink.submit(item);
          metrics.counter(MetricRegistry.name(type.name(), "failed-resubmit")).inc();
        }
        break;
      case HANDLED:
        // no-op
        if (log.isTraceEnabled()) {
          log.trace("result=handled");
        }
        metrics.counter(MetricRegistry.name(type.name(), "handled")).inc();
        break;
      case NOT_HANDLED_SHUTDOWN:
        if (log.isTraceEnabled()) {
          log.trace("result=nothandled_shutdown");
        }
        metrics.counter(MetricRegistry.name(type.name(), "nothandled_shutdown")).inc();
        break;
      case FAILED_RETRY:
        log.error(
            "Unexpected response while processing request. We never expect {}.", result.status());
        metrics.counter(MetricRegistry.name(type.name(), "failed-retry")).inc();
        break;
      default:
        if (log.isTraceEnabled()) {
          log.trace("result=no matching case");
        }
        // no-op
    }
  }

  /** Shutdown the Kafka consumer by calling wakeup. */
  public final void shutdown() {
    kafkaConsumer.wakeup();
    log.info("Shutdown called on KafkaCrossDcConsumer");
    try {
      if (!executor.isShutdown()) {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
      if (!offsetCheckExecutor.isShutdown()) {
        offsetCheckExecutor.shutdown();
        offsetCheckExecutor.awaitTermination(30, TimeUnit.SECONDS);
      }
      solrClient.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted while waiting for executor to shutdown");
    } catch (Exception e) {
      log.warn("Exception closing Solr client on shutdown", e);
    } finally {
      Util.logMetrics(metrics);
    }
  }

  protected CloudSolrClient createSolrClient(KafkaCrossDcConf conf) {
    return new CloudSolrClient.Builder(
            Collections.singletonList(conf.get(KafkaCrossDcConf.ZK_CONNECT_STRING)),
            Optional.empty())
        .build();
  }

  protected KafkaMirroringSink createKafkaMirroringSink(KafkaCrossDcConf conf) {
    return new KafkaMirroringSink(conf);
  }
}
