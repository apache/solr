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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.noggit.ObjectBuilder;
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

  @BeforeClass
  public static void beforeAll() {
    ensureCompatibleLocale();
  }

  @Before
  public void beforeSolrAndKafkaIntegrationTest() throws Exception {
    uceh = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(
        (t, e) -> log.error("Uncaught exception in thread {}", t, e));
    System.setProperty("otel.metrics.exporter", "prometheus");
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

    kafkaCluster.createTopic(TOPIC, 10, 1);

    // ensure small batches to test multi-partition ordering
    System.setProperty("batchSizeBytes", "128");
    System.setProperty("solr.crossdc.topicName", TOPIC);
    System.setProperty("solr.crossdc.bootstrapServers", kafkaCluster.bootstrapServers());
    System.setProperty(INDEX_UNMIRRORABLE_DOCS, "false");

    solrCluster1 =
        configureCluster(1).addConfig("conf", getFile("configs/cloud-minimal/conf")).configure();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(COLLECTION, 1, 1);

    solrCluster2 =
        configureCluster(1).addConfig("conf", getFile("configs/cloud-minimal/conf")).configure();

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

  @Test
  public void testFullCloudToCloud() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");

    client.add(doc);

    client.commit(COLLECTION);

    log.info("Sent producer record");

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 1);
  }

  @Test
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

  private static final String LOREM_IPSUM =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";

  @Test
  public void testStrictOrdering() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient();
    int NUM_DOCS = 5000;
    // delay deletes by this many docs
    int DELTA = 100;
    for (int i = 0; i < NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "id-" + i);
      doc.addField("text", "some test with a relatively long field. " + LOREM_IPSUM);

      client.add(COLLECTION, doc);
      if (i >= DELTA) {
        client.deleteById(COLLECTION, "id-" + (i - DELTA));
      }
    }

    // send the remaining deletes in random order
    ArrayList<Integer> ids = new ArrayList<>(DELTA);
    IntStream.range(0, DELTA).forEach(i -> ids.add(i));
    Collections.shuffle(ids, random());
    for (Integer id : ids) {
      client.deleteById(COLLECTION, "id-" + (NUM_DOCS - DELTA + id));
    }

    client.commit(COLLECTION);

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 0);
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

  @Test
  @SuppressWarnings({"unchecked"})
  public void testMetricsAndHealthcheck() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(new Date().getTime()));
    doc.addField("text", "some test");

    client.add(COLLECTION, doc);

    client.commit(COLLECTION);

    log.info("Sent producer record");

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 1);

    String baseUrl = "http://localhost:" + KafkaCrossDcConf.DEFAULT_PORT;
    HttpJettySolrClient httpJettySolrClient =
        new HttpJettySolrClient.Builder(baseUrl).useHttp1_1(true).build();
    try {
      // test the metrics endpoint
      GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/metrics");
      req.setResponseParser(new InputStreamResponseParser(null));
      NamedList<Object> rsp = httpJettySolrClient.request(req);
      String content =
          IOUtils.toString(
              (InputStream) rsp.get(InputStreamResponseParser.STREAM_KEY), StandardCharsets.UTF_8);
      assertTrue(content, content.contains("crossdc_consumer_output_total"));

      // test the healtcheck endpoint
      req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/health");
      req.setResponseParser(new InputStreamResponseParser(null));
      rsp = httpJettySolrClient.request(req);
      content =
          IOUtils.toString(
              (InputStream) rsp.get(InputStreamResponseParser.STREAM_KEY), StandardCharsets.UTF_8);
      assertEquals(Integer.valueOf(200), rsp.get("responseStatus"));
      Map<String, Object> map = (Map<String, Object>) ObjectBuilder.fromJSON(content);
      assertEquals(Boolean.TRUE, map.get("kafka"));
      assertEquals(Boolean.TRUE, map.get("solr"));
      assertEquals(Boolean.TRUE, map.get("running"));

      // kill Solr to trigger unhealthy state
      solrCluster2.shutdown();
      solrCluster2 = null;
      Thread.sleep(5000);
      rsp = httpJettySolrClient.request(req);
      content =
          IOUtils.toString(
              (InputStream) rsp.get(InputStreamResponseParser.STREAM_KEY), StandardCharsets.UTF_8);
      assertEquals(Integer.valueOf(503), rsp.get("responseStatus"));
      map = (Map<String, Object>) ObjectBuilder.fromJSON(content);
      assertEquals(Boolean.TRUE, map.get("kafka"));
      assertEquals(Boolean.FALSE, map.get("solr"));
      assertEquals(Boolean.TRUE, map.get("running"));

    } finally {
      httpJettySolrClient.close();
      client.close();
    }
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

  public static void ensureCompatibleLocale() {
    assumeTrue(
        "The current default locale is not compatible with the Kafka server",
        "CLASSIC".equals("classic".toUpperCase(Locale.getDefault())));
  }
}
