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
package org.apache.solr.cloud;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slow
@LuceneTestCase.Nightly // MRM TODO: look at setErrorHook
public class ChaosMonkeySafeLeaderTest extends SolrCloudBridgeTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));
  private ClusterChaosMonkey chaosMonkey;

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    System.setProperty("solr.skipCommitOnClose", "false");
    useFactory(null);
    schemaString = "schema15.xml";      // we need a string id (StoppableIndexingThread uses
    // "<thread>-<counter>" ids; the default collection1/conf/schema.xml has copyField id->id_i1
    // (and ->range_facet_l/_dv/i_dv), all numeric dynamic fields, so a string id fails every add
    // with NumberFormatException. schema15.xml has no id->numeric copyFields.)
    System.setProperty("solr.autoCommit.maxTime", "15000");
    System.setProperty("solr.httpclient.retries", "3");
    System.setProperty("solr.retries.on.forward", "3");
    System.setProperty("solr.retries.to.followers", "3");
    useFactory(null);
    System.setProperty("solr.suppressDefaultConfigBootstrap", "false");

    createControl = false;

    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "-1"));
    if (sliceCount == -1) {
      sliceCount = random().nextInt(TEST_NIGHTLY ? 5 : 3) + 1;
    }

    replicationFactor = 3;

    //    int numShards = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.shardcount", "-1"));
    //    if (numShards == -1) {
    //      // we make sure that there's at least one shard with more than one replica
    //      // so that the ChaosMonkey has something to kill
    //      numShards = sliceCount + random().nextInt(TEST_NIGHTLY ? 12 : 2) + 1;
    //    }
    numJettys = sliceCount * replicationFactor;
  }
  
  @AfterClass
  public static void afterSuperClass() {
    System.clearProperty("solr.autoCommit.maxTime");
    //clearErrorHook();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    chaosMonkey = new ClusterChaosMonkey(cluster, COLLECTION);
    //setErrorHook();
  }

  protected static final String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  protected static final RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};
  
  public String[] getFieldNames() {
    return fieldNames;
  }

  public RandVal[] getRandValues() {
    return randVals;
  }
  

  public ChaosMonkeySafeLeaderTest() throws Exception {
    super();
  }

  @Test
  public void test() throws Exception {
    
    handle.clear();
    handle.put("timestamp", SKIPVAL);
    
    // randomly turn on 1 seconds 'soft' commit
    //randomlyEnableAutoSoftCommit();
    cluster.waitForActiveCollection(COLLECTION, sliceCount, sliceCount * replicationFactor);

    //tryDelete();
    
    List<StoppableIndexingThread> threads = new ArrayList<>();
    int threadCount = 2;
    int batchSize = 1;
    if (random().nextBoolean()) {
      batchSize = random().nextInt(98) + 2;
    }
    
    boolean pauseBetweenUpdates = !TEST_NIGHTLY || random().nextBoolean();
    int maxUpdates = -1;
    if (!pauseBetweenUpdates) {
      maxUpdates = 1000 + random().nextInt(1000);
    } else {
      maxUpdates = 1500;
    }
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      StoppableIndexingThread indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true, maxUpdates, batchSize, pauseBetweenUpdates); // random().nextInt(999) + 1
      indexThread.setUseLongId(true);
      threads.add(indexThread);
      Future<?> future = ParWork.submit("StoppableIndexingThread", indexThread);
      futures.add(future);
    }
    
    chaosMonkey.startTheMonkey(false, 5000);
    try {
      long runLength;
      if (RUN_LENGTH != -1) {
        runLength = RUN_LENGTH;
      } else {
        int[] runTimes;
        if (TEST_NIGHTLY) {
          runTimes = new int[] {5000, 6000, 10000, 15000, 25000, 30000,
              30000, 45000};
        } else {
          runTimes = new int[] {15000};
        }
        runLength = runTimes[random().nextInt(runTimes.length)];
      }
      
      Thread.sleep(runLength);
    } finally {
      chaosMonkey.stopTheMonkey();
    }

    Thread.sleep(1000);

    for (StoppableIndexingThread indexThread : threads) {
      indexThread.safeStop();
    }
    
    // wait for stop...
    for (Future future : futures) {
      future.get();
    }
    
    for (StoppableIndexingThread indexThread : threads) {
      assertTrue(String.valueOf(indexThread.getFailCount()), indexThread.getFailCount() < 10);
    }

    while (true) {
      cluster.getSolrClient().getZkStateReader().waitForState(COLLECTION, 30, TimeUnit.SECONDS, (liveNodes, collectionState) -> {
        if (collectionState == null) return false;
        Collection<Slice> slices = collectionState.getSlices();
        for (Slice slice : slices) {
          for (Replica replica : slice.getReplicas()) {
            if (cluster.getSolrClient().getZkStateReader().isNodeLive(replica.getNodeName())) {
              if (replica.getState() != Replica.State.ACTIVE) {
                return false;
              }
            }
          }
        }
        return true;
      });

      Thread.sleep(1000);

      Collection<Slice> slices = cluster.getSolrClient().getZkStateReader().getCollectionOrNull(COLLECTION).getSlices();
      try {
        for (Slice slice : slices) {
          cluster.getSolrClient().getZkStateReader().getLeaderRetry(cluster.getSolrClient().getHttpClient(), COLLECTION, slice.getName(), 5000, true);
        }
        break;
      } catch (Exception e) {
        log.error("exception waiting for leaders", e);
        Thread.sleep(150);
      }
    }

    Thread.sleep(1000);

    try {
      commit();
    } catch (Exception e) {
      log.info("Exception in commit", e);
    }

    // MRM TODO: make test fail on compare fail
    try {
      cluster.getSolrClient().getZkStateReader().checkShardConsistency(COLLECTION);
    } catch (AssertionError fail) {
      Thread.sleep(500);
      commit();
      try {
        cluster.getSolrClient().getZkStateReader().checkShardConsistency(COLLECTION);
      } catch (AssertionError fail2) {
        Thread.sleep(1000);
        commit();
        cluster.getSolrClient().getZkStateReader().checkShardConsistency(COLLECTION);
      }
    }
    
    if (VERBOSE) System.out.println("control docs:" + controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() + "\n\n");
    
    // try and make a collection to make sure the overseer has survived the expiration and session loss

    // sometimes we restart zookeeper as well
//    if (TEST_NIGHTLY && random().nextBoolean()) {
//      zkServer.shutdown();
//      zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
//      zkServer.run(false);
//    }

//    try (CloudHttp2SolrClient client = createCloudClient("collection1")) {
//        createCollection(null, "testcollection", 1, 1, 1, client, null, "_default");
//
//    }
    List<Integer> numShardsNumReplicas = new ArrayList<>(2);
    numShardsNumReplicas.add(1);
    numShardsNumReplicas.add(1);
 //   checkForCollection("testcollection",numShardsNumReplicas, null);
  }

  private void tryDelete() throws Exception {
    long nanoTime = System.nanoTime();
    long start = System.nanoTime();
    long timeout = start + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    while (nanoTime < timeout) {
      try {
        del("*:*");
        break;
      } catch (SolrServerException e) {
        // cluster may not be up yet
        log.error("", e);
      }
      Thread.sleep(100);
      nanoTime = System.nanoTime();
    }
  }
  
  // skip the randoms - they can deadlock...
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    indexDoc(doc);
  }

}
