package org.apache.solr.crossdc.manager;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.crossdc.manager.consumer.KafkaCrossDcConsumer;
import org.apache.solr.crossdc.manager.consumer.PartitionManager;
import org.apache.solr.crossdc.manager.messageprocessor.SolrMessageProcessor;
import org.apache.solr.crossdc.test.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@ThreadLeakFilters(defaultFilters = true, filters = { SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class, SolrKafkaTestsIgnoredThreadsFilter.class })
@ThreadLeakLingering(linger = 5000) public class DeleteByQueryToIdTest extends
    SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
  protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer;

  protected static volatile List<MirroredSolrRequest> requests = new ArrayList<>();

  private static String TOPIC = "topic1";

  private static String COLLECTION1 = "collection1";
  private static String COLLECTION2 = "collection2";

  @BeforeClass
  public static void beforeSolrAndKafkaIntegrationTest() throws Exception {

    System.setProperty(KafkaCrossDcConf.PORT, "-1");
    consumer = new Consumer();
    System.setProperty("solr.crossdc.dbq_rows", "1");

    Properties config = new Properties();
    config.put("unclean.leader.election.enable", "true");
    config.put("enable.partition.eof", "false");

    kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS, config) {
      public String bootstrapServers() {
        return super.bootstrapServers().replaceAll("localhost", "127.0.0.1");
      }
    };
    kafkaCluster.start();

    kafkaCluster.createTopic(TOPIC, 1, 1);

    // System.setProperty("topicName", null);
    // System.setProperty("bootstrapServers", null);

    Properties props = new Properties();

    solrCluster1 = new SolrCloudTestCase.Builder(1, createTempDir())
        .addConfig("conf", getFile("src/test/resources/configs/cloud-minimal/conf").toPath())
        .addConfig("confNoDbq", getFile("src/test/resources/configs/cloud-minimal-no-dbq/conf").toPath())
        .configure();

    props.setProperty("topicName", TOPIC);
    props.setProperty("bootstrapServers", kafkaCluster.bootstrapServers());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    props.store(baos, "");
    byte[] data = baos.toByteArray();
    solrCluster1.getZkClient().makePath("/crossdc.properties", data, true);

    CollectionAdminRequest.Create createSource1 =
        CollectionAdminRequest.createCollection(COLLECTION1, "conf", 1, 1);
    solrCluster1.getSolrClient().request(createSource1);
    solrCluster1.waitForActiveCollection(COLLECTION1, 1, 1);
    CollectionAdminRequest.Create createSource2 =
        CollectionAdminRequest.createCollection(COLLECTION2, "confNoDbq", 1, 1);
    solrCluster1.getSolrClient().request(createSource2);
    solrCluster1.waitForActiveCollection(COLLECTION2, 1, 1);

    solrCluster1.getSolrClient().setDefaultCollection(COLLECTION1);

    solrCluster2 = new SolrCloudTestCase.Builder(1, createTempDir())
        .addConfig("conf", getFile("src/test/resources/configs/cloud-minimal/conf").toPath())
        .addConfig("confNoDbq", getFile("src/test/resources/configs/cloud-minimal-no-dbq/conf").toPath())
        .configure();

    solrCluster2.getSolrClient().getZkStateReader().getZkClient().makePath("/crossdc.properties", data, true);

    CollectionAdminRequest.Create createTarget1 =
        CollectionAdminRequest.createCollection(COLLECTION1, "conf", 1, 1);
    solrCluster2.getSolrClient().request(createTarget1);
    solrCluster2.waitForActiveCollection(COLLECTION1, 1, 1);

    CollectionAdminRequest.Create createTarget2 =
        CollectionAdminRequest.createCollection(COLLECTION2, "confNoDbq", 1, 1);
    solrCluster2.getSolrClient().request(createTarget2);
    solrCluster2.waitForActiveCollection(COLLECTION2, 1, 1);
    solrCluster2.getSolrClient().setDefaultCollection(COLLECTION2);

    String bootstrapServers = kafkaCluster.bootstrapServers();
    log.info("bootstrapServers={}", bootstrapServers);

    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, bootstrapServers);
    properties.put(KafkaCrossDcConf.ZK_CONNECT_STRING, solrCluster2.getZkServer().getZkAddress());
    properties.put(KafkaCrossDcConf.TOPIC_NAME, TOPIC);
    properties.put(KafkaCrossDcConf.GROUP_ID, "group1");

    consumer = new Consumer() {
      @Override
      protected CrossDcConsumer getCrossDcConsumer(KafkaCrossDcConf conf, CountDownLatch startLatch) {
        return new KafkaCrossDcConsumer(conf, startLatch) {
          @Override
          protected SolrMessageProcessor createSolrMessageProcessor() {
            return new SolrMessageProcessor(solrClient, resubmitRequest -> 0L) {
              @Override
              public Result<MirroredSolrRequest> handleItem(MirroredSolrRequest mirroredSolrRequest) {
                requests.add(mirroredSolrRequest);
                return super.handleItem(mirroredSolrRequest);
              }
            };
          }
        };
      }
    };
    consumer.start(properties);
  }

  @AfterClass
  public static void afterSolrAndKafkaIntegrationTest() throws Exception {
    ObjectReleaseTracker.clear();

    consumer.shutdown();

    try {
      kafkaCluster.stop();
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }

    if (solrCluster1 != null) {
      solrCluster1.getZkServer().getZkClient().printLayoutToStream(System.out);
      solrCluster1.shutdown();
    }
    if (solrCluster2 != null) {
      solrCluster2.getZkServer().getZkClient().printLayoutToStream(System.out);
      solrCluster2.shutdown();
    }

    solrCluster1 = null;
    solrCluster2 = null;
    kafkaCluster = null;
    consumer = null;
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    solrCluster1.getSolrClient().deleteByQuery(COLLECTION, "*:*");
    solrCluster2.getSolrClient().deleteByQuery(COLLECTION, "*:*");
    solrCluster1.getSolrClient().commit(COLLECTION);
    solrCluster2.getSolrClient().commit(COLLECTION);
    requests.clear();
  }

  @Test
  public void testExpandDBQ() throws Exception {

    List<SolrInputDocument> docs = new ArrayList<>();
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");
    docs.add(doc);

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", String.valueOf(System.nanoTime()));
    doc2.addField("text", "some test two");
    docs.add(doc2);

    SolrInputDocument doc3= new SolrInputDocument();
    doc3.addField("id", String.valueOf(System.nanoTime()));
    doc3.addField("text", "two of a kind");
    docs.add(doc3);

    SolrInputDocument doc4= new SolrInputDocument();
    doc4.addField("id", String.valueOf(System.nanoTime()));
    doc4.addField("text", "one two three");
    docs.add(doc4);

    CloudSolrClient client = solrCluster1.getSolrClient();
    client.add(docs);
    client.commit(COLLECTION1);
    // add also to the other collection
    client.add(COLLECTION2, docs);
    client.commit(COLLECTION2);

    client.deleteByQuery("text:two");
    client.deleteByQuery(COLLECTION2, "text:two");

    client.commit(COLLECTION1);
    client.commit(COLLECTION2);

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 50; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION1);
      solrCluster1.getSolrClient().query(COLLECTION1, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION1, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 1) {
        foundUpdates = true;
      } else {
        Thread.sleep(1000);
      }
    }
    assertTrue("results=" + results, foundUpdates);
    assertEquals("requests=" + requests, 4, requests.size());
    UpdateRequest ureq = (UpdateRequest) requests.get(0).getSolrRequest();
    assertEquals("update1/col1=" + ureq, COLLECTION1, ureq.getParams().get("collection"));
    assertEquals("update1/col1=" + ureq, 4, ureq.getDocuments().size());
    ureq = (UpdateRequest) requests.get(1).getSolrRequest();
    assertEquals("update1/col2=" + ureq, COLLECTION2, ureq.getParams().get("collection"));
    assertEquals("update1/col2=" + ureq, 4, ureq.getDocuments().size());    ureq = (UpdateRequest) requests.get(1).getSolrRequest();
    ureq = (UpdateRequest) requests.get(2).getSolrRequest();
    assertEquals("update2/col1=" + ureq, COLLECTION1, ureq.getParams().get("collection"));
    assertEquals("update2/col1.dbi=" + ureq, 3, ureq.getDeleteById() != null ? ureq.getDeleteById().size() : 0);
    assertEquals("update2/col1.dbq=" + ureq, 0, ureq.getDeleteQuery() != null ? ureq.getDeleteQuery().size() : 0);
    ureq = (UpdateRequest) requests.get(3).getSolrRequest();
    assertEquals("update2/col2=" + ureq, COLLECTION2, ureq.getParams().get("collection"));
    assertEquals("update2/col2.dbi=" + ureq, 0, ureq.getDeleteById() != null ? ureq.getDeleteById().size() : 0);
    assertEquals("update2/col2.dbq=" + ureq, 1, ureq.getDeleteQuery() != null ? ureq.getDeleteQuery().size() : 0);
  }

}
