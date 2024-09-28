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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      SolrKafkaTestsIgnoredThreadsFilter.class
    })
@ThreadLeakLingering(linger = 5000)
@LuceneTestCase.Nightly // this test is currently too slow for non-nightly
public class RetryQueueIntegrationTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
  protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer;

  private static String TOPIC = "topic1";

  private static String COLLECTION = "collection1";
  private static Path baseDir1;
  private static Path baseDir2;
  private static ZkTestServer zkTestServer1;
  private static ZkTestServer zkTestServer2;

  @BeforeClass
  public static void beforeRetryQueueIntegrationTestTest() throws Exception {

    System.setProperty(KafkaCrossDcConf.PORT, "-1");
    consumer = new Consumer();

    Properties config = new Properties();
    config.put("unclean.leader.election.enable", "true");
    config.put("enable.partition.eof", "false");

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

    baseDir1 = createTempDir();
    Path zkDir1 = baseDir1.resolve("zookeeper/server1/data");
    zkTestServer1 = new ZkTestServer(zkDir1);
    try {
      zkTestServer1.run();
    } catch (Exception e) {
      log.error("Error starting Zk Test Server, trying again ...");
      zkTestServer1.shutdown();
      zkTestServer1 = new ZkTestServer(zkDir1);
      zkTestServer1.run();
    }

    baseDir2 = createTempDir();
    Path zkDir2 = baseDir2.resolve("zookeeper/server1/data");
    zkTestServer2 = new ZkTestServer(zkDir2);
    try {
      zkTestServer2.run();
    } catch (Exception e) {
      log.error("Error starting Zk Test Server, trying again ...");
      zkTestServer2.shutdown();
      zkTestServer2 = new ZkTestServer(zkDir2);
      zkTestServer2.run();
    }

    solrCluster1 = startCluster(solrCluster1, zkTestServer1, baseDir1);
    solrCluster2 = startCluster(solrCluster2, zkTestServer2, baseDir2);

    CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);

    String bootstrapServers = kafkaCluster.bootstrapServers();
    log.info("bootstrapServers={}", bootstrapServers);

    Map<String, Object> properties = new HashMap<>();
    properties.put(KafkaCrossDcConf.BOOTSTRAP_SERVERS, bootstrapServers);
    properties.put(KafkaCrossDcConf.ZK_CONNECT_STRING, solrCluster2.getZkServer().getZkAddress());
    properties.put(KafkaCrossDcConf.TOPIC_NAME, TOPIC);
    properties.put(KafkaCrossDcConf.GROUP_ID, "group1");
    properties.put(KafkaCrossDcConf.MAX_ATTEMPTS, 100);
    consumer.start(properties);
  }

  private static MiniSolrCloudCluster startCluster(
      MiniSolrCloudCluster solrCluster, ZkTestServer zkTestServer, Path baseDir) throws Exception {
    MiniSolrCloudCluster cluster =
        new MiniSolrCloudCluster(
            1,
            baseDir,
            MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML,
            JettyConfig.builder().withSSLConfig(sslConfig.buildServerSSLConfig()).build(),
            zkTestServer,
            false);

    // new SolrCloudTestCase.Builder(1, baseDir).addConfig("conf",
    // getFile("configs/cloud-minimal/conf").toPath()).configure();
    cluster.uploadConfigSet(getFile("configs/cloud-minimal/conf").toPath(), "conf");

    CollectionAdminRequest.Create create2 =
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1);
    cluster.getSolrClient().request(create2);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
    return cluster;
  }

  @AfterClass
  public static void afterRetryQueueIntegrationTest() throws Exception {
    ObjectReleaseTracker.clear();

    consumer.shutdown();

    try {
      kafkaCluster.stop();
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }

    if (solrCluster1 != null) {
      solrCluster1.shutdown();
    }
    if (solrCluster2 != null) {
      solrCluster2.shutdown();
    }

    if (zkTestServer1 != null) {
      zkTestServer1.shutdown();
    }
    if (zkTestServer2 != null) {
      zkTestServer2.shutdown();
    }

    consumer = null;
    solrCluster1 = null;
    solrCluster2 = null;
    kafkaCluster = null;
    zkTestServer1 = null;
    zkTestServer2 = null;
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    solrCluster1.getSolrClient().deleteByQuery(COLLECTION, "*:*");
    solrCluster2.getSolrClient().deleteByQuery(COLLECTION, "*:*");
    solrCluster1.getSolrClient().commit(COLLECTION);
    solrCluster2.getSolrClient().commit(COLLECTION);
  }

  @Test
  public void testRetryQueueSolrDown() throws Exception {
    solrCluster2.getJettySolrRunner(0).stop();

    CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");

    client.add(COLLECTION, doc);

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", String.valueOf(System.nanoTime()));
    doc2.addField("text", "some test");

    client.add(COLLECTION, doc2);

    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField("id", String.valueOf(System.nanoTime()));
    doc3.addField("text", "some test");

    client.add(COLLECTION, doc3);

    client.commit(COLLECTION);

    System.out.println("Sent producer record");

    Thread.sleep(15000);

    solrCluster2.getJettySolrRunner(0).start();
    Thread.sleep(10000);

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 200; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 3) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    assertTrue("expected updates not found, results=" + results, foundUpdates);
  }

  @Test
  public void testRetryQueueZKDown() throws Exception {
    Path zkDir = zkTestServer2.getZkDir();
    int zkPort = zkTestServer2.getPort();
    zkTestServer2.shutdown();

    CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.nanoTime()));
    doc.addField("text", "some test");

    client.add(COLLECTION, doc);

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField("id", String.valueOf(System.nanoTime()));
    doc2.addField("text", "some test");

    client.add(COLLECTION, doc2);

    SolrInputDocument doc3 = new SolrInputDocument();
    doc3.addField("id", String.valueOf(System.nanoTime()));
    doc3.addField("text", "some test");

    client.add(COLLECTION, doc3);

    client.commit(COLLECTION);

    Thread.sleep(15000);

    zkTestServer2 = new ZkTestServer(zkDir, zkPort);
    zkTestServer2.run(false);

    Thread.sleep(10000);

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 15; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 3) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    assertTrue("expected updates not found, results=" + results, foundUpdates);
  }
}
