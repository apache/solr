package org.apache.solr.crossdc.manager;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.crossdc.test.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

  protected static volatile Consumer consumer = new Consumer();

  private static String TOPIC = "topic1";

  private static String COLLECTION = "collection1";

  @BeforeClass
  public static void beforeSolrAndKafkaIntegrationTest() throws Exception {

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

    solrCluster1 = configureCluster(1).addConfig("conf",
        getFile("configs/cloud-minimal/conf").toPath()).configure();

    props.setProperty("topicName", TOPIC);
    props.setProperty("bootstrapServers", kafkaCluster.bootstrapServers());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    props.store(baos, "");
    byte[] data = baos.toByteArray();
    solrCluster1.getZkClient().makePath("/crossdc.properties", data, true);

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    solrCluster1.getSolrClient().request(create);
    solrCluster1.waitForActiveCollection(COLLECTION, 1, 1);

    solrCluster2 = configureCluster(1).addConfig("conf",
        getFile("configs/cloud-minimal/conf").toPath()).configure();

    solrCluster2.getZkClient().makePath("/crossdc.properties", data, true);

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
  }

  @Test
  public void testDBQ() throws Exception {

    CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");
    client.add(doc);

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", String.valueOf(System.nanoTime()));
    doc2.addField("text", "some test two");
    client.add(doc2);

    SolrInputDocument doc3= new SolrInputDocument();
    doc3.addField("id", String.valueOf(System.nanoTime()));
    doc3.addField("text", "two of a kind");
    client.add(doc3);

    SolrInputDocument doc4= new SolrInputDocument();
    doc4.addField("id", String.valueOf(System.nanoTime()));
    doc4.addField("text", "one two three");
    client.add(doc4);

    client.commit(COLLECTION);

    client.deleteByQuery("two");

    client.commit(COLLECTION);

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 50; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 1) {
        foundUpdates = true;
      } else {
        Thread.sleep(1000);
      }
    }

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);

  }

}
