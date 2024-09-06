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
package org.apache.solr.crossdc.manager;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.DEFAULT_MAX_REQUEST_SIZE;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.INDEX_UNMIRRORABLE_DOCS;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.PORT;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      SolrKafkaTestsIgnoredThreadsFilter.class
    })
@ThreadLeakLingering(linger = 5000)
public class SolrAndKafkaIntegrationTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_DOC_SIZE_BYTES = Integer.parseInt(DEFAULT_MAX_REQUEST_SIZE);

  private static final int NUM_BROKERS = 1;
  public EmbeddedKafkaCluster kafkaCluster;

  protected volatile MiniSolrCloudCluster solrCluster1;
  protected volatile MiniSolrCloudCluster solrCluster2;

  protected volatile Consumer consumer;

  private static final String TOPIC = "topic1";

  private static final String COLLECTION = "collection1";
  private static final String ALT_COLLECTION = "collection2";
  private static Thread.UncaughtExceptionHandler uceh;

  @Before
  public void beforeSolrAndKafkaIntegrationTest() throws Exception {
    uceh = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(
        (t, e) -> log.error("Uncaught exception in thread {}", t, e));
    System.setProperty(PORT, "-1");
    consumer = new Consumer();
    Properties config = new Properties();

    kafkaCluster =
        new EmbeddedKafkaCluster(NUM_BROKERS, config) {
          @Override
          public String bootstrapServers() {
            return super.bootstrapServers().replaceAll("localhost", "127.0.0.1");
          }
        };
    kafkaCluster.start();

    kafkaCluster.createTopic(TOPIC, 1, 1);

    System.setProperty("solr.crossdc.topicName", TOPIC);
    System.setProperty("solr.crossdc.bootstrapServers", kafkaCluster.bootstrapServers());
    System.setProperty(INDEX_UNMIRRORABLE_DOCS, "false");

    solrCluster1 =
        configureCluster(1)
            .addConfig("conf", getFile("configs/cloud-minimal/conf").toPath())
            .configure();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(COLLECTION, 1, 1);

    solrCluster2 =
        configureCluster(1)
            .addConfig("conf", getFile("configs/cloud-minimal/conf").toPath())
            .configure();

    CollectionAdminRequest.Create create2 =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster2.getSolrClient().request(create2);
    solrCluster2.waitForActiveCollection(COLLECTION, 1, 1);

    String bootstrapServers = kafkaCluster.bootstrapServers();
    log.info("bootstrapServers={}", bootstrapServers);

    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, bootstrapServers);
    properties.put(KafkaCrossDcConf.ZK_CONNECT_STRING, solrCluster2.getZkServer().getZkAddress());
    properties.put(KafkaCrossDcConf.TOPIC_NAME, TOPIC);
    properties.put(KafkaCrossDcConf.GROUP_ID, "group1");
    properties.put(KafkaCrossDcConf.MAX_REQUEST_SIZE_BYTES, MAX_DOC_SIZE_BYTES);
    consumer.start(properties);
  }

  @After
  public void afterSolrAndKafkaIntegrationTest() throws Exception {
    ObjectReleaseTracker.clear();

    if (solrCluster1 != null) {
      solrCluster1.getZkServer().getZkClient().printLayoutToStream(System.out);
      solrCluster1.shutdown();
    }
    if (solrCluster2 != null) {
      solrCluster2.getZkServer().getZkClient().printLayoutToStream(System.out);
      solrCluster2.shutdown();
    }

    consumer.shutdown();
    consumer = null;

    try {
      // kafkaCluster.deleteAllTopicsAndWait(5000);
      kafkaCluster.stop();
      kafkaCluster = null;
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }

    Thread.setDefaultUncaughtExceptionHandler(uceh);
  }

  public void testFullCloudToCloud() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");

    client.add(doc);

    client.commit(COLLECTION);

    System.out.println("Sent producer record");

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 1);
  }

  public void testProducerToCloud() throws Exception {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", kafkaCluster.bootstrapServers());
    properties.put("acks", "all");
    properties.put("retries", 1);
    properties.put("batch.size", 1);
    properties.put("buffer.memory", 33554432);
    properties.put("linger.ms", 1);
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", MirroredSolrRequestSerializer.class.getName());
    Producer<String, MirroredSolrRequest<?>> producer = new KafkaProducer<>(properties);
    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.setParam("shouldMirror", "true");
    updateRequest.add("id", String.valueOf(System.nanoTime()), "text", "test");
    updateRequest.add("id", String.valueOf(System.nanoTime() + 22), "text", "test2");
    updateRequest.setParam("collection", COLLECTION);
    MirroredSolrRequest<?> mirroredSolrRequest = new MirroredSolrRequest<>(updateRequest);
    producer.send(
        new ProducerRecord<>(TOPIC, mirroredSolrRequest),
        (metadata, exception) ->
            log.warn("Producer finished sending metadata={}, exception={}", metadata, exception));
    producer.flush();

    solrCluster2.getSolrClient().commit(COLLECTION);

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 2);

    producer.close();
  }

  @Test
  @Ignore("This relies on collection properties and I don't see where they are read anymore")
  public void testMirroringUpdateProcessor() throws Exception {
    final SolrInputDocument tooLargeDoc = new SolrInputDocument();
    tooLargeDoc.addField("id", System.nanoTime());
    tooLargeDoc.addField(
        "text", new String(new byte[2 * MAX_DOC_SIZE_BYTES], StandardCharsets.UTF_8));
    final SolrInputDocument normalDoc = new SolrInputDocument();
    normalDoc.addField("id", System.nanoTime() + 22);
    normalDoc.addField("text", "Hello world");
    List<SolrInputDocument> docsToIndex = new ArrayList<>();
    docsToIndex.add(normalDoc);
    docsToIndex.add(tooLargeDoc);

    final CloudSolrClient cluster1Client = solrCluster1.getSolrClient(COLLECTION);
    try {
      cluster1Client.add(docsToIndex);
    } catch (CloudSolrClient.RouteException e) {
      // expected
    }
    cluster1Client.commit(COLLECTION);

    // Primary and secondary should each only index 'normalDoc'
    final String normalDocQuery = "id:" + normalDoc.get("id").getFirstValue();
    assertCluster2EventuallyHasDocs(COLLECTION, normalDocQuery, 1);
    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 1);
    assertClusterEventuallyHasDocs(cluster1Client, COLLECTION, normalDocQuery, 1);
    assertClusterEventuallyHasDocs(cluster1Client, COLLECTION, "*:*", 1);

    // Create new primary+secondary collection where 'tooLarge' docs ARE indexed on the primary
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(ALT_COLLECTION, "conf", 1, 1)
            .withProperty(INDEX_UNMIRRORABLE_DOCS, "true");
    try {
      solrCluster1.getSolrClient().request(create);
      solrCluster2.getSolrClient().request(create);
      solrCluster1.waitForActiveCollection(ALT_COLLECTION, 1, 1);
      solrCluster2.waitForActiveCollection(ALT_COLLECTION, 1, 1);

      cluster1Client.add(ALT_COLLECTION, docsToIndex);
      cluster1Client.commit(ALT_COLLECTION);

      // try adding another doc
      //      final SolrInputDocument newDoc = new SolrInputDocument();
      //
      //      newDoc.addField("id", System.nanoTime());
      //      newDoc.addField("text", "Hello world");
      //      docsToIndex = new ArrayList<>();
      //      docsToIndex.add(newDoc);
      //
      //    try {
      //      cluster1Client.add(ALT_COLLECTION, docsToIndex);
      //    } catch (BaseCloudSolrClient.RouteException e) {
      //      // expected
      //    }
      //      cluster1Client.commit(ALT_COLLECTION);

      // Primary should have both 'normal' and 'large' docs; secondary should only have 'normal'
      // doc.
      assertClusterEventuallyHasDocs(cluster1Client, ALT_COLLECTION, "*:*", 2);
      assertCluster2EventuallyHasDocs(ALT_COLLECTION, "*:*", 1);
      assertCluster2EventuallyHasDocs(ALT_COLLECTION, normalDocQuery, 1);
    } finally {
      CollectionAdminRequest.Delete delete =
          CollectionAdminRequest.deleteCollection(ALT_COLLECTION);
      solrCluster1.getSolrClient().request(delete);
      solrCluster2.getSolrClient().request(delete);
    }
  }

  @Test
  public void testParallelUpdatesToCluster2() throws Exception {
    ExecutorService executorService =
        ExecutorUtil.newMDCAwareFixedThreadPool(12, new SolrNamedThreadFactory("test"));
    List<Future<Boolean>> futures = new ArrayList<>();

    CloudSolrClient client1 = solrCluster1.getSolrClient(COLLECTION);

    // Prepare and send 500 updates in parallel
    for (int i = 0; i < 5000; i++) {
      final int docId = i;
      Future<Boolean> future =
          executorService.submit(
              () -> {
                try {
                  SolrInputDocument doc = new SolrInputDocument();
                  doc.addField("id", String.valueOf(docId));
                  doc.addField("text", "parallel test");
                  client1.add(doc);
                  return true;
                } catch (Exception e) {
                  log.error("Exception while adding doc", e);
                  return false;
                }
              });
      futures.add(future);
    }

    // Wait for all updates to complete
    ExecutorUtil.shutdownAndAwaitTermination(executorService);

    // Check if all updates were successful
    for (Future<Boolean> future : futures) {
      assertTrue(future.get());
    }

    client1.commit(COLLECTION);

    // Check if these documents are correctly reflected in the second cluster
    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 5000);
  }

  private void assertCluster2EventuallyHasDocs(String collection, String query, int expectedNumDocs)
      throws Exception {
    assertClusterEventuallyHasDocs(
        solrCluster2.getSolrClient(), collection, query, expectedNumDocs);
  }

  private void assertClusterEventuallyHasDocs(
      SolrClient client, String collection, String query, int expectedNumDocs) throws Exception {
    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 100; i++) {
      client.commit(collection);
      results = client.query(collection, new SolrQuery(query));
      if (results.getResults().getNumFound() == expectedNumDocs) {
        foundUpdates = true;
      } else {
        Thread.sleep(200);
      }
    }

    assertTrue("results=" + results, foundUpdates);
  }
}
