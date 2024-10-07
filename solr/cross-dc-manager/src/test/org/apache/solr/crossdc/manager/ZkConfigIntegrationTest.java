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
import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.manager.consumer.Consumer;
import org.apache.solr.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
@SuppressForbidden(reason = "test")
public class ZkConfigIntegrationTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String VERSION_FIELD = "_version_";

  private static final int NUM_BROKERS = 1;
  public static EmbeddedKafkaCluster kafkaCluster;

  protected static volatile MiniSolrCloudCluster solrCluster1;
  protected static volatile MiniSolrCloudCluster solrCluster2;

  protected static volatile Consumer consumer1;
  protected static volatile Consumer consumer2;

  private static String TOPIC1 = "topicSrc";
  private static String TOPIC2 = "topicDst";

  private static String COLLECTION = "collection1";

  @BeforeClass
  public static void beforeSolrAndKafkaIntegrationTest() throws Exception {
    System.setProperty(KafkaCrossDcConf.PORT, "-1");
    consumer1 = new Consumer();
    consumer2 = new Consumer();
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

    kafkaCluster.createTopic(TOPIC1, 1, 1);
    kafkaCluster.createTopic(TOPIC2, 1, 1);

    // System.setProperty("solr.crossdc.topicName", null);
    // System.setProperty("solr.crossdc.bootstrapServers", null);

    Properties props = new Properties();

    solrCluster1 =
        configureCluster(1)
            .addConfig("conf", getFile("configs/cloud-minimal/conf").toPath())
            .configure();

    props.setProperty(KafkaCrossDcConf.TOPIC_NAME, TOPIC2);
    props.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, kafkaCluster.bootstrapServers());

    System.setProperty(KafkaCrossDcConf.TOPIC_NAME, TOPIC2);
    System.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, kafkaCluster.bootstrapServers());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    props.store(baos, "");
    byte[] data = baos.toByteArray();
    solrCluster1.getZkClient().makePath("/crossdc.properties", data, true);

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

    props = new Properties();
    props.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, kafkaCluster.bootstrapServers());

    baos = new ByteArrayOutputStream();
    props.store(baos, "");
    data = baos.toByteArray();
    solrCluster2.getZkClient().makePath("/crossdc.properties", data, true);

    String bootstrapServers = kafkaCluster.bootstrapServers();
    log.info("bootstrapServers={}", bootstrapServers);

    Map<String, Object> properties = new HashMap<>();
    Object put =
        properties.put(
            KafkaCrossDcConf.ZK_CONNECT_STRING, solrCluster2.getZkServer().getZkAddress());

    System.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, kafkaCluster.bootstrapServers());

    consumer1.start(properties);

    System.setProperty(
        KafkaCrossDcConf.ZK_CONNECT_STRING, solrCluster1.getZkServer().getZkAddress());
    System.setProperty(KafkaCrossDcConf.TOPIC_NAME, TOPIC2);
    System.setProperty(KafkaCrossDcConf.PORT, "8383");
    consumer2.start();
  }

  @AfterClass
  public static void afterSolrAndKafkaIntegrationTest() throws Exception {
    ObjectReleaseTracker.clear();

    if (solrCluster1 != null) {
      solrCluster1.getZkClient().printLayoutToStream(System.out);
      solrCluster1.shutdown();
    }
    if (solrCluster2 != null) {
      solrCluster2.getZkClient().printLayoutToStream(System.out);
      solrCluster2.shutdown();
    }

    consumer1.shutdown();
    consumer2.shutdown();

    try {
      kafkaCluster.stop();
    } catch (Exception e) {
      log.error("Exception stopping Kafka cluster", e);
    }

    solrCluster1 = null;
    solrCluster2 = null;
    kafkaCluster = null;
    consumer1 = null;
    consumer2 = null;
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

  public void testConfigFromZkPickedUp() throws Exception {
    CloudSolrClient client = solrCluster1.getSolrClient(COLLECTION);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.currentTimeMillis()));
    doc.addField("text", "some test");

    client.add(COLLECTION, doc);

    client.commit(COLLECTION);

    System.out.println("Sent producer record");

    QueryResponse results = null;
    boolean foundUpdates = false;
    for (int i = 0; i < 300; i++) {
      solrCluster2.getSolrClient().commit(COLLECTION);
      solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 1) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    System.out.println("Closed producer");

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);

    client = solrCluster2.getSolrClient();
    doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(System.currentTimeMillis()));
    doc.addField("text", "some test2");

    client.add(COLLECTION, doc);

    client.commit(COLLECTION);

    System.out.println("Sent producer record");

    results = null;
    foundUpdates = false;
    for (int i = 0; i < 100; i++) {
      solrCluster1.getSolrClient().commit(COLLECTION);
      solrCluster2.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      results = solrCluster1.getSolrClient().query(COLLECTION, new SolrQuery("*:*"));
      if (results.getResults().getNumFound() == 1) {
        foundUpdates = true;
      } else {
        Thread.sleep(100);
      }
    }

    System.out.println("Closed producer");

    assertTrue("results=" + results, foundUpdates);
    System.out.println("Rest: " + results);
  }
}
