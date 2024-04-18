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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.manager.messageprocessor.SolrMessageProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class KafkaCrossDcConsumerTest {

  private KafkaCrossDcConsumer kafkaCrossDcConsumer;
  private KafkaConsumer<String, MirroredSolrRequest<?>> kafkaConsumerMock;
  private CloudSolrClient solrClientMock;
  private KafkaMirroringSink kafkaMirroringSinkMock;

  private SolrMessageProcessor messageProcessorMock;

  private KafkaCrossDcConf conf;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUp() {
    kafkaConsumerMock = mock(KafkaConsumer.class);
    solrClientMock = mock(CloudSolrClient.class);
    kafkaMirroringSinkMock = mock(KafkaMirroringSink.class);
    messageProcessorMock = mock(SolrMessageProcessor.class);
    conf = testCrossDCConf();
    // Set necessary configurations

    kafkaCrossDcConsumer =
        new KafkaCrossDcConsumer(conf, new CountDownLatch(0)) {
          @Override
          public KafkaConsumer<String, MirroredSolrRequest<?>> createKafkaConsumer(
              Properties properties) {
            return kafkaConsumerMock;
          }

          @Override
          public SolrMessageProcessor createSolrMessageProcessor() {
            return messageProcessorMock;
          }

          @Override
          protected CloudSolrClient createSolrClient(KafkaCrossDcConf conf) {
            return solrClientMock;
          }

          @Override
          protected KafkaMirroringSink createKafkaMirroringSink(KafkaCrossDcConf conf) {
            return kafkaMirroringSinkMock;
          }
        };
  }

  private static KafkaCrossDcConf testCrossDCConf(String... keyValues) {
    Map<String, Object> config = new HashMap<>();
    config.put(KafkaCrossDcConf.TOPIC_NAME, "topic1");
    config.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "localhost:9092");
    if (keyValues != null) {
      for (int i = 0; i < keyValues.length; i += 2) {
        config.put(keyValues[i], keyValues[i + 1]);
      }
    }
    return new KafkaCrossDcConf(config);
  }

  @After
  public void tearDown() {
    kafkaCrossDcConsumer.shutdown();
  }

  private ConsumerRecord<String, MirroredSolrRequest<?>> createSampleConsumerRecord() {
    return new ConsumerRecord<>("sample-topic", 0, 0, "key", createSampleMirroredSolrRequest());
  }

  private ConsumerRecords<String, MirroredSolrRequest<?>> createSampleConsumerRecords() {
    TopicPartition topicPartition = new TopicPartition("sample-topic", 0);
    List<ConsumerRecord<String, MirroredSolrRequest<?>>> recordsList = new ArrayList<>();
    recordsList.add(
        new ConsumerRecord<>("sample-topic", 0, 0, "key", createSampleMirroredSolrRequest()));
    return new ConsumerRecords<>(Collections.singletonMap(topicPartition, recordsList));
  }

  private MirroredSolrRequest<?> createSampleMirroredSolrRequest() {
    // Create a sample MirroredSolrRequest for testing
    SolrInputDocument solrInputDocument = new SolrInputDocument();
    solrInputDocument.addField("id", "1");
    solrInputDocument.addField("title", "Sample title");
    solrInputDocument.addField("content", "Sample content");
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(solrInputDocument);
    return new MirroredSolrRequest<>(updateRequest);
  }

  /** Should create a KafkaCrossDcConsumer with the given configuration and startLatch */
  @Test
  public void kafkaCrossDcConsumerCreationWithConfigurationAndStartLatch() {
    CountDownLatch startLatch = new CountDownLatch(1);
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    KafkaCrossDcConsumer kafkaCrossDcConsumer =
        spy(
            new KafkaCrossDcConsumer(conf, startLatch) {
              @Override
              public KafkaConsumer<String, MirroredSolrRequest<?>> createKafkaConsumer(
                  Properties properties) {
                return mockConsumer;
              }

              @Override
              public SolrMessageProcessor createSolrMessageProcessor() {
                return messageProcessorMock;
              }

              @Override
              protected KafkaMirroringSink createKafkaMirroringSink(KafkaCrossDcConf conf) {
                return kafkaMirroringSinkMock;
              }
            });

    assertNotNull(kafkaCrossDcConsumer);
    assertEquals(1, startLatch.getCount());
  }

  @Test
  public void testRunAndShutdown() throws Exception {
    // Define the expected behavior of the mocks and set up the test scenario

    // Use a CountDownLatch to wait for the KafkaConsumer.subscribe method to be called
    CountDownLatch subscribeLatch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              subscribeLatch.countDown();
              return null;
            })
        .when(kafkaConsumerMock)
        .subscribe(anyList());

    when(kafkaConsumerMock.poll(any())).thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

    ExecutorService consumerThreadExecutor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("CrossDCConsumer"));

    // Run the test
    consumerThreadExecutor.submit(kafkaCrossDcConsumer);

    // Wait for the KafkaConsumer.subscribe method to be called
    assertTrue(subscribeLatch.await(10, TimeUnit.SECONDS));

    // Run the shutdown method
    kafkaCrossDcConsumer.shutdown();

    // Verify that the consumer was subscribed with the correct topic names
    verify(kafkaConsumerMock).subscribe(anyList());

    // Verify that the appropriate methods were called on the mocks
    verify(kafkaConsumerMock).wakeup();
    verify(solrClientMock).close();

    consumerThreadExecutor.shutdown();
    consumerThreadExecutor.awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test
  public void testHandleFailedResubmit() throws Exception {
    // Set up the KafkaCrossDcConsumer
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    KafkaCrossDcConsumer consumer = createCrossDcConsumerSpy(mockConsumer);

    doNothing()
        .when(consumer)
        .sendBatch(
            any(UpdateRequest.class),
            eq(MirroredSolrRequest.Type.UPDATE),
            any(ConsumerRecord.class),
            any(PartitionManager.WorkUnit.class));

    // Set up the SolrMessageProcessor mock
    SolrMessageProcessor mockMessageProcessor = mock(SolrMessageProcessor.class);
    MirroredSolrRequest<?> request = new MirroredSolrRequest<>(new UpdateRequest());
    IQueueHandler.Result<MirroredSolrRequest<?>> failedResubmitResult =
        new IQueueHandler.Result<>(IQueueHandler.ResultStatus.FAILED_RESUBMIT, null, request);
    when(mockMessageProcessor.handleItem(any(MirroredSolrRequest.class)))
        .thenReturn(failedResubmitResult);

    // Mock the KafkaMirroringSink
    KafkaMirroringSink mockKafkaMirroringSink = mock(KafkaMirroringSink.class);
    doNothing().when(mockKafkaMirroringSink).submit(any(MirroredSolrRequest.class));
    consumer.kafkaMirroringSink = mockKafkaMirroringSink;

    // Call the method to test
    consumer.processResult(MirroredSolrRequest.Type.UPDATE, failedResubmitResult);

    // Verify that the KafkaMirroringSink.submit() method was called
    verify(consumer.kafkaMirroringSink, times(1)).submit(request);
  }

  @Test
  public void testCreateKafkaCrossDcConsumer() {
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    KafkaCrossDcConsumer consumer = createCrossDcConsumerSpy(mockConsumer);

    assertNotNull(consumer);
  }

  @Test
  public void testHandleValidMirroredSolrRequest() {
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    KafkaCrossDcConsumer spyConsumer = createCrossDcConsumerSpy(mockConsumer);
    doReturn(new IQueueHandler.Result<>(IQueueHandler.ResultStatus.HANDLED))
        .when(messageProcessorMock)
        .handleItem(any());
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    UpdateRequest validRequest = new UpdateRequest();
    validRequest.add(doc);
    validRequest.setParams(new ModifiableSolrParams().add("commit", "true"));
    // Create a valid MirroredSolrRequest
    ConsumerRecord<String, MirroredSolrRequest<?>> record =
        new ConsumerRecord<>("test-topic", 0, 0, "key", new MirroredSolrRequest<>(validRequest));
    ConsumerRecords<String, MirroredSolrRequest<?>> records =
        new ConsumerRecords<>(
            Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(record)));

    when(mockConsumer.poll(any())).thenReturn(records).thenThrow(new WakeupException());

    spyConsumer.run();

    // Verify that the valid MirroredSolrRequest was processed.
    verify(spyConsumer, times(1))
        .sendBatch(
            argThat(
                solrRequest -> {
                  // Check if the UpdateRequest has the same content as the original validRequest
                  return ((UpdateRequest) solrRequest)
                          .getDocuments()
                          .equals(validRequest.getDocuments())
                      && solrRequest
                          .getParams()
                          .toNamedList()
                          .equals(validRequest.getParams().toNamedList());
                }),
            eq(MirroredSolrRequest.Type.UPDATE),
            eq(record),
            any());
  }

  @Test
  public void testHandleValidAdminRequest() throws Exception {
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    KafkaCrossDcConsumer spyConsumer = createCrossDcConsumerSpy(mockConsumer);
    doReturn(new IQueueHandler.Result<>(IQueueHandler.ResultStatus.HANDLED))
        .when(messageProcessorMock)
        .handleItem(any());
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection("test", "testConfig", 2, 2);

    ConsumerRecord<String, MirroredSolrRequest<?>> record1 =
        new ConsumerRecord<>(
            "test-topic",
            0,
            0,
            "key1",
            new MirroredSolrRequest<>(MirroredSolrRequest.Type.ADMIN, create));
    ConsumerRecord<String, MirroredSolrRequest<?>> record2 =
        new ConsumerRecord<>(
            "test-topic",
            0,
            1,
            "key2",
            new MirroredSolrRequest<>(MirroredSolrRequest.Type.UPDATE, new UpdateRequest()));
    ConsumerRecords<String, MirroredSolrRequest<?>> records =
        new ConsumerRecords<>(
            Collections.singletonMap(
                new TopicPartition("test-topic", 0), List.of(record1, record2)));

    when(mockConsumer.poll(any())).thenReturn(records).thenThrow(new WakeupException());

    spyConsumer.run();

    // Verify that the valid MirroredSolrRequest was processed.
    verify(spyConsumer, times(1))
        .sendBatch(
            argThat(
                solrRequest -> {
                  // Check if the SolrRequest has the same content as the original validRequest
                  return solrRequest
                      .getParams()
                      .toNamedList()
                      .equals(create.getParams().toNamedList());
                }),
            eq(MirroredSolrRequest.Type.ADMIN),
            eq(record1),
            any());
    verify(spyConsumer, times(1))
        .sendBatch(any(), eq(MirroredSolrRequest.Type.UPDATE), eq(record2), any());
  }

  @Test
  public void testCollapseUpdatesNONE() {
    int NUM_REQS = 100;
    doTestCollapseUpdates(CrossDcConf.CollapseUpdates.NONE, 500, NUM_REQS, NUM_REQS, 0);
  }

  @Test
  public void testCollapseUpdatesALL() {
    int NUM_REQS = 100;
    doTestCollapseUpdates(CrossDcConf.CollapseUpdates.ALL, 500, NUM_REQS, 1, 5);
  }

  @Test
  public void testCollapseUpdatesALLMaxCollapse() {
    int NUM_REQS = 100;
    doTestCollapseUpdates(CrossDcConf.CollapseUpdates.ALL, 50, NUM_REQS, 2, 5);
  }

  @Test
  public void testCollapseUpdatesPARTIAL() {
    int NUM_REQS = 100;
    doTestCollapseUpdates(CrossDcConf.CollapseUpdates.PARTIAL, 500, NUM_REQS, 10, 5);
  }

  private void doTestCollapseUpdates(
      CrossDcConf.CollapseUpdates collapseUpdates,
      int maxCollapseRecords,
      int inputReqs,
      int outputReqs,
      int reqsWithDeletes) {
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    // override
    conf =
        testCrossDCConf(
            CrossDcConf.COLLAPSE_UPDATES, collapseUpdates.name(),
            CrossDcConf.MAX_COLLAPSE_RECORDS, String.valueOf(maxCollapseRecords));
    KafkaCrossDcConsumer spyConsumer = createCrossDcConsumerSpy(mockConsumer);
    doReturn(new IQueueHandler.Result<>(IQueueHandler.ResultStatus.HANDLED))
        .when(messageProcessorMock)
        .handleItem(any());
    List<ConsumerRecord<String, MirroredSolrRequest<?>>> records = new ArrayList<>();
    for (int i = 0; i < inputReqs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "id-" + i);
      UpdateRequest validRequest = new UpdateRequest();
      validRequest.add(doc);
      if ((i % 3) == 0 && reqsWithDeletes > 0) {
        validRequest.deleteById("fakeId-" + i);
        reqsWithDeletes--;
      }
      // Create a valid MirroredSolrRequest
      ConsumerRecord<String, MirroredSolrRequest<?>> record =
          new ConsumerRecord<>("test-topic", 0, 0, "key", new MirroredSolrRequest<>(validRequest));
      records.add(record);
    }
    ConsumerRecords<String, MirroredSolrRequest<?>> consumerRecords =
        new ConsumerRecords<>(
            Collections.singletonMap(new TopicPartition("test-topic", 0), records));

    when(mockConsumer.poll(any())).thenReturn(consumerRecords).thenThrow(new WakeupException());

    spyConsumer.run();

    // Verify that the valid MirroredSolrRequest was processed.
    verify(spyConsumer, times(outputReqs))
        .sendBatch(any(), eq(MirroredSolrRequest.Type.UPDATE), any(), any());
  }

  @Test
  public void testHandleInvalidMirroredSolrRequest() {
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    SolrMessageProcessor mockSolrMessageProcessor = mock(SolrMessageProcessor.class);
    doReturn(new IQueueHandler.Result<>(IQueueHandler.ResultStatus.HANDLED))
        .when(mockSolrMessageProcessor)
        .handleItem(any());
    KafkaCrossDcConsumer spyConsumer =
        spy(
            new KafkaCrossDcConsumer(conf, new CountDownLatch(1)) {
              @Override
              public KafkaConsumer<String, MirroredSolrRequest<?>> createKafkaConsumer(
                  Properties properties) {
                return mockConsumer;
              }

              @Override
              public SolrMessageProcessor createSolrMessageProcessor() {
                return mockSolrMessageProcessor;
              }

              @Override
              protected KafkaMirroringSink createKafkaMirroringSink(KafkaCrossDcConf conf) {
                return kafkaMirroringSinkMock;
              }
            });
    doReturn(mockConsumer).when(spyConsumer).createKafkaConsumer(any());

    UpdateRequest invalidRequest = new UpdateRequest();
    // no updates on request
    invalidRequest.setParams(new ModifiableSolrParams().add("invalid_param", "invalid_value"));

    ConsumerRecord<String, MirroredSolrRequest<?>> record =
        new ConsumerRecord<>("test-topic", 0, 0, "key", new MirroredSolrRequest<>(invalidRequest));
    ConsumerRecords<String, MirroredSolrRequest<?>> records =
        new ConsumerRecords<>(
            Collections.singletonMap(new TopicPartition("test-topic", 0), List.of(record)));

    when(mockConsumer.poll(any())).thenReturn(records).thenThrow(new WakeupException());

    spyConsumer.run();

    // Verify that the valid MirroredSolrRequest was processed.
    verify(spyConsumer, times(1))
        .sendBatch(
            argThat(
                solrRequest -> {
                  System.out.println(Utils.toJSONString(solrRequest));
                  // Check if the UpdateRequest has the same content as the original invalidRequest
                  return ((UpdateRequest) solrRequest).getDocuments() == null
                      && solrRequest
                          .getParams()
                          .toNamedList()
                          .equals(invalidRequest.getParams().toNamedList());
                }),
            any(),
            eq(record),
            any());
  }

  @Test
  public void testHandleWakeupException() {
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    KafkaCrossDcConsumer spyConsumer = createCrossDcConsumerSpy(mockConsumer);

    when(mockConsumer.poll(any())).thenThrow(new WakeupException());

    // Run the consumer in a separate thread to avoid blocking the test
    ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("test"));
    executorService.submit(spyConsumer);

    // Wait for a short period to allow the consumer to start and then trigger the shutdown
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    spyConsumer.shutdown();

    // Verify that the WakeupException was caught and handled
    verify(mockConsumer, atLeastOnce()).poll(any());
    verify(mockConsumer, times(1)).wakeup();

    // Shutdown the executor service
    executorService.shutdown();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  public void testShutdown() {
    KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer = mock(KafkaConsumer.class);
    KafkaCrossDcConsumer spyConsumer = createCrossDcConsumerSpy(mockConsumer);

    spyConsumer.shutdown();

    verify(mockConsumer, times(1)).wakeup();
  }

  private KafkaCrossDcConsumer createCrossDcConsumerSpy(
      KafkaConsumer<String, MirroredSolrRequest<?>> mockConsumer) {
    return spy(
        new KafkaCrossDcConsumer(conf, new CountDownLatch(1)) {
          @Override
          public KafkaConsumer<String, MirroredSolrRequest<?>> createKafkaConsumer(
              Properties properties) {
            return mockConsumer;
          }

          @Override
          public SolrMessageProcessor createSolrMessageProcessor() {
            return messageProcessorMock;
          }

          @Override
          protected KafkaMirroringSink createKafkaMirroringSink(KafkaCrossDcConf conf) {
            return kafkaMirroringSinkMock;
          }
        });
  }
}
