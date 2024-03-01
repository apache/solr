package org.apache.solr.crossdc.manager;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.crossdc.manager.consumer.Util;
import org.apache.solr.crossdc.test.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.crossdc.common.KafkaCrossDcConf.DEFAULT_MAX_REQUEST_SIZE;
import static org.apache.solr.crossdc.common.KafkaCrossDcConf.INDEX_UNMIRRORABLE_DOCS;

@ThreadLeakFilters(defaultFilters = true, filters = { SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class })
@ThreadLeakLingering(linger = 5000) public class SolrAndKafkaMultiCollectionIntegrationTest extends
    SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_DOC_SIZE_BYTES = Integer.parseInt(DEFAULT_MAX_REQUEST_SIZE);

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public EmbeddedKafkaCluster kafkaCluster;

  protected volatile MiniSolrCloudCluster solrCluster1;
  protected volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer;

  private static String TOPIC = "topic1";

  private static String COLLECTION = "collection1";
  private static String ALT_COLLECTION = "collection2";

  @Before
  public void beforeSolrAndKafkaIntegrationTest() throws Exception {
    System.setProperty(KafkaCrossDcConf.PORT, "-1");
    consumer = new Consumer();
    Properties config = new Properties();
    //config.put("unclean.leader.election.enable", "true");
    //config.put("enable.partition.eof", "false");

    kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS, config) {
      public String bootstrapServers() {
        return super.bootstrapServers().replaceAll("localhost", "127.0.0.1");
      }
    };
    kafkaCluster.start();

    kafkaCluster.createTopic(TOPIC, 1, 1);

    // in this test we will count on collection properties for topicName and enabled=true
    System.setProperty("enabled", "false");
    // System.setProperty("topicName", TOPIC);

    System.setProperty("bootstrapServers", kafkaCluster.bootstrapServers());
    System.setProperty(INDEX_UNMIRRORABLE_DOCS, "false");

    solrCluster1 = configureCluster(1).addConfig("conf",
        getFile("configs/cloud-minimal/conf").toPath()).configure();

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(COLLECTION, 1, 1);

    solrCluster2 = configureCluster(1).addConfig("conf",
        getFile("configs/cloud-minimal/conf").toPath()).configure();

    CollectionAdminRequest.Create create2 =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster2.getSolrClient().request(create2);
    solrCluster2.waitForActiveCollection(COLLECTION, 1, 1);

    // Update the collection property "enabled" to true
    CollectionProperties cp = new CollectionProperties(solrCluster1.getZkClient());
    cp.setCollectionProperty(COLLECTION, "crossdc.enabled", "true");
    cp.setCollectionProperty(COLLECTION, "crossdc.topicName", TOPIC);
    // Reloading the collection
    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(COLLECTION);
    reloadRequest.process(solrCluster1.getSolrClient());

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

    Util.printKafkaInfo(kafkaCluster.bootstrapServers(), "SolrCrossDCConsumer");

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
      //kafkaCluster.deleteAllTopicsAndWait(10000);
      kafkaCluster.stop();
      kafkaCluster = null;
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }


  }

  private static SolrInputDocument getDoc() {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");
    return doc;
  }

  private void assertCluster2EventuallyHasDocs(String collection, String query, int expectedNumDocs) throws Exception {
    assertClusterEventuallyHasDocs(solrCluster2.getSolrClient(), collection, query, expectedNumDocs);
  }

  private void createCollection(CloudSolrClient client, CollectionAdminRequest.Create createCmd) throws Exception {
    final String stashedDefault = client.getDefaultCollection();
    try {
      //client.setDefaultCollection(null);
      client.request(createCmd);
    } finally {
      //client.setDefaultCollection(stashedDefault);
    }
  }

  @Test
  public void testFullCloudToCloudMultiCollection() throws Exception {
    CollectionAdminRequest.Create create =
            CollectionAdminRequest.createCollection(ALT_COLLECTION, "conf", 1, 1);

    try {
      solrCluster1.getSolrClient().request(create);
      solrCluster1.waitForActiveCollection(ALT_COLLECTION, 1, 1);

      solrCluster2.getSolrClient().request(create);
      solrCluster2.waitForActiveCollection(ALT_COLLECTION, 1, 1);

      // Update the collection property "enabled" to true
      CollectionProperties cp = new CollectionProperties(solrCluster1.getZkClient());
      cp.setCollectionProperty(ALT_COLLECTION, "crossdc.enabled", "true");
      cp.setCollectionProperty(ALT_COLLECTION, "crossdc.topicName", TOPIC);
      // Reloading the collection
      CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(ALT_COLLECTION);
      reloadRequest.process(solrCluster1.getSolrClient());


      CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);

      SolrInputDocument doc1 = getDoc();
      SolrInputDocument doc2 = getDoc();
      SolrInputDocument doc3 = getDoc();
      SolrInputDocument doc4 = getDoc();
      SolrInputDocument doc5 = getDoc();
      SolrInputDocument doc6 = getDoc();
      SolrInputDocument doc7 = getDoc();

      client.add(COLLECTION, doc1);
      client.add(ALT_COLLECTION, doc2);
      client.add(COLLECTION, doc3);
      client.add(COLLECTION, doc4);
      client.add(ALT_COLLECTION, doc5);
      client.add(ALT_COLLECTION, doc6);
      client.add(COLLECTION, doc7);

      client.commit(COLLECTION);
      client.commit(ALT_COLLECTION);

      System.out.println("Sent producer record");

      assertCluster2EventuallyHasDocs(ALT_COLLECTION, "*:*", 3);
      assertCluster2EventuallyHasDocs(COLLECTION, "*:*", 4);

    } finally {
      CollectionAdminRequest.Delete delete =
              CollectionAdminRequest.deleteCollection(ALT_COLLECTION);
      solrCluster1.getSolrClient().request(delete);
      solrCluster2.getSolrClient().request(delete);
    }
  }
@Test
public void testParallelUpdatesToMultiCollections() throws Exception {
  CollectionAdminRequest.Create create =
          CollectionAdminRequest.createCollection(ALT_COLLECTION, "conf", 1, 1);

  try {
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(ALT_COLLECTION, 1, 1);

    solrCluster2.getSolrClient().request(create);
    solrCluster2.waitForActiveCollection(ALT_COLLECTION, 1, 1);

    // Update the collection property "enabled" to true
    CollectionProperties cp = new CollectionProperties(solrCluster1.getZkClient());
    cp.setCollectionProperty(ALT_COLLECTION, "crossdc.enabled", "true");
    cp.setCollectionProperty(ALT_COLLECTION, "crossdc.topicName", TOPIC);
    // Reloading the collection
    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(ALT_COLLECTION);
    reloadRequest.process(solrCluster1.getSolrClient());

    ExecutorService executorService = Executors.newFixedThreadPool(24);
    List<Future<Boolean>> futures = new ArrayList<>();

    CloudSolrClient client1 = solrCluster1.getSolrClient(COLLECTION);

    // Prepare and send N updates to COLLECTION and N updates to ALT_COLLECTION in parallel
    int updates = 25000;
    for (int i = 0; i < updates; i++) {
      final int docIdForCollection = i;
      final int docIdForAltCollection = i + updates;

      Future<Boolean> futureForCollection = executorService.submit(() -> {
        try {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", String.valueOf(docIdForCollection));
          doc.addField("text", "parallel test for COLLECTION");
          client1.add(COLLECTION, doc);
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          log.error("Exception while adding doc to COLLECTION", e);
          return false;
        }
      });

      Future<Boolean> futureForAltCollection = executorService.submit(() -> {
        try {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", String.valueOf(docIdForAltCollection));
          doc.addField("text", "parallel test for ALT_COLLECTION");
          client1.add(ALT_COLLECTION, doc);
          return true;
        } catch (Exception e) {
          e.printStackTrace();
          log.error("Exception while adding doc to ALT_COLLECTION", e);
          return false;
        }
      });

      futures.add(futureForCollection);
      futures.add(futureForAltCollection);
    }

    // Wait for all updates to complete
    executorService.shutdown();
    if (!executorService.awaitTermination(1600, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }

    // Check if all updates were successful
    for (Future<Boolean> future : futures) {
      assertTrue(future.get());
    }

    client1.commit(COLLECTION);
    client1.commit(ALT_COLLECTION);

    // Check if these documents are correctly reflected in the second cluster
    assertCluster2EventuallyHasDocs(ALT_COLLECTION, "*:*", updates);
    assertCluster2EventuallyHasDocs(COLLECTION, "*:*", updates);

  } finally {
    CollectionAdminRequest.Delete delete =
            CollectionAdminRequest.deleteCollection(ALT_COLLECTION);
    solrCluster1.getSolrClient().request(delete);
    solrCluster2.getSolrClient().request(delete);
  }
}


  private void assertClusterEventuallyHasDocs (SolrClient client, String collection, String query,int expectedNumDocs) throws
  Exception {
    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 1500; i++) {
      client.commit(collection);
      results = client.query(collection, new SolrQuery(query));
      if (results.getResults().getNumFound() == expectedNumDocs) {
        foundUpdates = true;
      } else {
        Thread.sleep(500);
      }
    }

    assertTrue("results=" + results, foundUpdates);
  }
}
