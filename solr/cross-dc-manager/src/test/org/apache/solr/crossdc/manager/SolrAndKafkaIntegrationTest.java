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

import static org.apache.solr.crossdc.common.CrossDcConf.COLLAPSE_UPDATES;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.BATCH_SIZE_BYTES;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.BOOTSTRAP_SERVERS;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.DEFAULT_MAX_REQUEST_SIZE;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.INDEX_UNMIRRORABLE_DOCS;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.TOPIC_NAME;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
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
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.crossdc.manager.consumer.ConsumerMetrics;
import org.apache.solr.crossdc.manager.consumer.KafkaCrossDcConsumer;
import org.apache.solr.crossdc.manager.consumer.PartitionManager;
import org.apache.solr.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

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
  public KafkaContainer kafkaContainer;

  private static class ConsumerBatch {
    final String kafkaTopic;
    final int partitionId;
    final MirroredSolrRequest.Type type;
    final String collection;
    final Map<String, String> headers;
    final Set<String> addIds = new HashSet<>();
    final String json;

    public ConsumerBatch(final MirroredSolrRequest.Type type, final SolrRequest<?> solrRequest) {
      this.kafkaTopic = solrRequest.getHeaders().get("record.topic");
      this.partitionId = Integer.parseInt(solrRequest.getHeaders().get("record.partition"));
      this.type = type;
      this.collection = solrRequest.getCollection();
      this.headers = solrRequest.getHeaders();
      if (solrRequest instanceof UpdateRequest) {
        UpdateRequest updateReq = (UpdateRequest) solrRequest;
        json =
            Utils.toJSONString(
                Map.of("params", updateReq.getParams(), "add", updateReq.getDocuments()));
        updateReq.getDocuments().forEach(doc -> addIds.add(doc.getFieldValue("id").toString()));
      } else {
        json =
            Utils.toJSONString(
                Map.of("params", solrRequest.getParams(), "class", solrRequest.getClass()));
      }
    }

    @Override
    public String toString() {
      return "ConsumerBatch{"
          + "kafkaTopic='"
          + kafkaTopic
          + '\''
          + ", partitionId="
          + partitionId
          + ", type="
          + type
          + ", collection='"
          + collection
          + '\''
          + ", headers="
          + headers
          + '\''
          + ", json='"
          + json
          + '\''
          + '}';
    }
  }

  protected volatile MiniSolrCloudCluster solrCluster1;
  protected volatile MiniSolrCloudCluster solrCluster2;

  protected volatile Consumer consumer;

  private BlockingQueue<ConsumerBatch> consumerBatches;

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
    System.setProperty(KafkaCrossDcConsumer.PROP_TOPIC_DEBUG, "true");
    consumerBatches = new LinkedBlockingQueue<>();
    consumer =
        new Consumer() {
          @Override
          protected CrossDcConsumer getCrossDcConsumer(
              final KafkaCrossDcConf conf,
              final ConsumerMetrics metrics,
              final CountDownLatch startLatch) {
            return new KafkaCrossDcConsumer(conf, metrics, startLatch) {
              @Override
              public void sendBatch(
                  final SolrRequest<? extends SolrResponse> solrReqBatch,
                  final MirroredSolrRequest.Type type,
                  final ConsumerRecord<String, MirroredSolrRequest<?>> lastRecord,
                  final PartitionManager.WorkUnit workUnit) {
                consumerBatches.offer(new ConsumerBatch(type, solrReqBatch));
                super.sendBatch(solrReqBatch, type, lastRecord, workUnit);
              }
            };
          }
        };
    Properties config = new Properties();

    kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
    kafkaContainer.start();

    String bootstrapServers = kafkaContainer.getBootstrapServers();

    // Replaced legacy in-JVM topic provisioner with official AdminClient configurations
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient adminClient = AdminClient.create(config)) {
      adminClient.createTopics(Collections.singletonList(
          new NewTopic(TOPIC, 3, (short) 1)
      )).all().get();
    }

    // Ensure small batches to test multi-partition ordering
    System.setProperty(BATCH_SIZE_BYTES, "100");
    System.setProperty(TOPIC_NAME, TOPIC);
    System.setProperty(BOOTSTRAP_SERVERS, bootstrapServers);
    System.setProperty(INDEX_UNMIRRORABLE_DOCS, "false");
    System.setProperty(COLLAPSE_UPDATES, "none");

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

    if (consumer != null) {
      consumer.shutdown();
      consumer = null;
    }

    if (kafkaContainer != null) {
      try {
        kafkaContainer.stop();
        kafkaContainer = null;
      } catch (Exception e) {
        log.error("Exception stopping Kafka container", e);
      }
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
    properties.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
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
  public void testPartitioning() throws Exception {
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(ALT_COLLECTION, "conf", 1, 1);
    create.process(solrCluster1.getSolrClient());
    create.process(solrCluster2.getSolrClient());
    solrCluster1.waitForActiveCollection(ALT_COLLECTION, 1, 1);
    solrCluster2.waitForActiveCollection(ALT_COLLECTION, 1, 1);

    CloudSolrClient client = solrCluster1.getSolrClient();
    int NUM_DOCS = 200;
    for (int i = 0; i < NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "id-" + i);
      doc.addField("id_i", i);
      doc.addField("text", "some test with a relatively long field. " + LOREM_IPSUM);
      doc.addField("collection_t", COLLECTION);

      client.add(COLLECTION, doc);

      doc = new SolrInputDocument();
      doc.addField("id", "id-" + i);
      doc.addField("id_i", i);
      doc.addField("text", "some test with a relatively long field. " + LOREM_IPSUM);
      doc.addField("collection_t", ALT_COLLECTION);

      client.add(ALT_COLLECTION, doc);
    }
    client.commit(COLLECTION);
    client.commit(ALT_COLLECTION);

    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", NUM_DOCS);
    assertCluster2EventuallyHasDocs(ALT_COLLECTION, "*:*", NUM_DOCS);

    // check that updates to different collections were always sent to the same partition
    Map<Integer, String> partitionsPerCol = new HashMap<>();
    Map<String, Set<String>> docsPerCol = new HashMap<>();
    int batchCount = 0;
    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.CURRENT_TIME);
    while (!timeOut.hasTimedOut()) {
      final ConsumerBatch batch = consumerBatches.poll(2, TimeUnit.SECONDS);
      if (batch == null) {
        int totalDocsSeen = docsPerCol.values().stream().mapToInt(Set::size).sum();
        if (totalDocsSeen == NUM_DOCS * 2) {
          // we've collected all expected docs
          break;
        } else {
          continue; // keep waiting, it was just a longer pause
        }
      }
      batchCount++;
      String collection =
          partitionsPerCol.computeIfAbsent(batch.partitionId, k -> batch.collection);
      docsPerCol.computeIfAbsent(collection, col -> new HashSet<>()).addAll(batch.addIds);
      assertEquals(
          "request in partition "
              + batch.partitionId
              + " has wrong collection "
              + batch.collection
              + ": "
              + batch
              + "\npartitions: "
              + partitionsPerCol,
          collection,
          batch.collection);
    }
    if (timeOut.hasTimedOut()) {
      fail("timed out waiting for batches");
    }
    assertTrue("No batches were received from consumer", batchCount > 0);
    assertEquals("Should have processed both collections", 2, docsPerCol.size());
    assertTrue("COLLECTION not found in results", docsPerCol.containsKey(COLLECTION));
    assertTrue("ALT_COLLECTION not found in results", docsPerCol.containsKey(ALT_COLLECTION));
    assertEquals(
        "incorrect count in collection " + COLLECTION, NUM_DOCS, docsPerCol.get(COLLECTION).size());
    assertEquals(
        "incorrect count in collection " + ALT_COLLECTION,
        NUM_DOCS,
        docsPerCol.get(ALT_COLLECTION).size());
  }

  @Test
  public void testStrictOrdering() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient();
    int NUM_DOCS = 1000;
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
      assertTrue(content, content.contains("solr_crossdc_consumer_output_total"));

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
      results =
          client.query(collection, new SolrQuery(CommonParams.Q, query, CommonParams.FL, "*"));
      if (results.getResults().getNumFound() == expectedNumDocs) {
        foundUpdates = true;
      } else {
        Thread.sleep(300);
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
