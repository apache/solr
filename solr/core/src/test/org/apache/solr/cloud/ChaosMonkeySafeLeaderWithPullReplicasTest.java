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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@LuceneTestCase.Nightly
public class ChaosMonkeySafeLeaderWithPullReplicasTest extends SolrCloudBridgeTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Integer RUN_LENGTH = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.runlength", "-1"));

  private static int numPullReplicas;
  private static int numRealtimeOrTlogReplicas;

  private ClusterChaosMonkey chaosMonkey;

  @Override
  protected int getPullReplicaCount() {
    return numPullReplicas;
  }

  @Override
  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
  }

  @BeforeClass
  public static void beforeSuperClass() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    if (usually()) {
      System.setProperty("solr.autoCommit.maxTime", "15000");
    }
    System.clearProperty("solr.httpclient.retries");
    System.clearProperty("solr.retries.on.forward");
    System.clearProperty("solr.retries.to.followers");
    useFactory(null);
    setErrorHook();

    numPullReplicas = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    numRealtimeOrTlogReplicas = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    sliceCount = Integer.parseInt(System.getProperty("solr.tests.cloud.cm.slicecount", "-1"));
    if (sliceCount == -1) {
      sliceCount = random().nextInt(TEST_NIGHTLY ? 3 : 2) + 1;
    }

    int numNodes = sliceCount * (numRealtimeOrTlogReplicas + numPullReplicas);
    numJettys = numNodes;
    replicationFactor = numRealtimeOrTlogReplicas;
    log.info("Starting ChaosMonkey test with {} shards and {} nodes", sliceCount, numNodes);
  }

  @AfterClass
  public static void afterSuperClass() {
    System.clearProperty("solr.autoCommit.maxTime");
    clearErrorHook();
    TestInjection.reset();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    useFactory("solr.StandardDirectoryFactory");
    super.setUp();
    chaosMonkey = new ClusterChaosMonkey(cluster, COLLECTION);
  }

  protected static final String[] fieldNames = new String[]{"f_i", "f_f", "f_d", "f_l", "f_dt"};
  protected static final RandVal[] randVals = new RandVal[]{rint, rfloat, rdouble, rlong, rdate};

  public String[] getFieldNames() {
    return fieldNames;
  }

  public RandVal[] getRandValues() {
    return randVals;
  }

  @Test
  //2018-06-18 (commented) @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void test() throws Exception {
    DocCollection docCollection = cloudClient.getZkStateReader().getClusterState().getCollection(COLLECTION);
    assertEquals(this.sliceCount, docCollection.getSlices().size());
    Slice s = docCollection.getSlice("s1");
    assertNotNull(s);
    assertEquals("Unexpected number of replicas. Collection: " + docCollection, numRealtimeOrTlogReplicas + numPullReplicas, s.getReplicas().size());
    assertEquals("Unexpected number of pull replicas. Collection: " + docCollection, numPullReplicas, s.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
    assertEquals(useTlogReplicas()?0:numRealtimeOrTlogReplicas, s.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
    assertEquals(useTlogReplicas()?numRealtimeOrTlogReplicas:0, s.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    // randomly turn on 1 seconds 'soft' commit
    randomlyEnableAutoSoftCommit();

    tryDelete();

    List<StoppableThread> threads = new ArrayList<>();
    int threadCount = 2;
    int batchSize = 1;
    if (random().nextBoolean()) {
      batchSize = random().nextInt(98) + 2;
    }

    boolean pauseBetweenUpdates = TEST_NIGHTLY ? random().nextBoolean() : true;
    int maxUpdates = -1;
    if (!pauseBetweenUpdates) {
      maxUpdates = 1000 + random().nextInt(1000);
    } else {
      maxUpdates = 15000;
    }

    ArrayList<Future> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      StoppableIndexingThread indexThread = new StoppableIndexingThread(controlClient, cloudClient, Integer.toString(i), true, maxUpdates, batchSize, pauseBetweenUpdates); // random().nextInt(999) + 1
      threads.add(indexThread);
      Future<?> future = ParWork.submit("StoppableSearchThread", indexThread);
      futures.add(future);
    }

    StoppableCommitThread commitThread = new StoppableCommitThread(cloudClient, 1000, false);
    threads.add(commitThread);
    Future<?> future = ParWork.submit("StoppableCommitThread", commitThread);
    futures.add(future);

    chaosMonkey.startTheMonkey(false, 500);
    try {
      long runLength;
      if (RUN_LENGTH != -1) {
        runLength = RUN_LENGTH;
      } else {
        int[] runTimes;
        if (TEST_NIGHTLY) {
          runTimes = new int[] {5000, 6000, 10000, 15000, 25000, 30000,
              30000, 45000, 90000, 120000};
        } else {
          runTimes = new int[] {5000, 7000, 15000};
        }
        runLength = runTimes[random().nextInt(runTimes.length - 1)];
      }

      ChaosMonkey.wait(runLength, COLLECTION, cloudClient.getZkStateReader());
    } finally {
      chaosMonkey.stopTheMonkey();
    }

    for (StoppableThread thread : threads) {
      thread.safeStop();
    }

    // wait for stop...
    for (Future thread : futures) {
      thread.get();
    }

    for (StoppableThread thread : threads) {
      if (thread instanceof StoppableIndexingThread) {
        assertEquals(0, ((StoppableIndexingThread) thread).getFailCount());
      }
    }

    if (log.isInfoEnabled()) {
      log.info("control docs:{}\n\n",
          controlClient != null ? controlClient.query(new SolrQuery("*:*")).getResults().getNumFound() : "N/A");
      log.info("collection state: {}", printClusterStateInfo(COLLECTION)); // logOk
    }

    // Final hard commit once indexing has stopped, BEFORE waiting for replicas and checking
    // consistency. The periodic StoppableCommitThread commits mid-run, but the indexing threads keep
    // adding docs after its last commit. Those trailing docs are durable in the leader's tlog and
    // visible to the leader's NRT searcher (q=*:*), but they are NOT yet in any index commit point --
    // so PULL replicas (which can only replicate a commit point) cannot see them, and
    // waitForReplicationFromReplicas only matches the leader's last-commit indexVersion, which never
    // advances to include those trailing docs without a new commit. That leaves the leader serving
    // more docs than the replicas can replicate -> the flaky "PULL replica count short of the leader"
    // mismatch, even though no docs were lost. Forcing a final commit rolls the trailing tlog docs into
    // a new commit point the replicas then replicate, quiescing the cluster so every live ACTIVE replica
    // converges to the same committed view. Mirrors the explicit commit() the sibling
    // ChaosMonkeySafeLeaderTest / ChaosMonkeyNothingIsSafeWithPullReplicasTest run before the check.
    commit();

    waitForReplicationFromReplicas(COLLECTION, cloudClient.getZkStateReader(), new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME));
//    waitForAllWarmingSearchers();

    checkShardConsistency(batchSize == 1, true);

    // try and make a collection to make sure the overseer has survived the expiration and session loss

    // sometimes we restart zookeeper as well
    if (random().nextBoolean()) {
      restartZk(0);
    }

    try (CloudHttp2SolrClient client = createCloudClient("collection1")) {
        createCollection(null, "testcollection", 1, 1, 100, client, null, "_default");
    }
    List<Integer> numShardsNumReplicas = new ArrayList<>(2);
    numShardsNumReplicas.add(1);
    numShardsNumReplicas.add(1 + getPullReplicaCount());
    checkForCollection("testcollection", numShardsNumReplicas, null);
  }

  private void tryDelete() throws Exception {
    long start = System.nanoTime();
    long timeout = start + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      try {
        del("*:*");
        break;
      } catch (SolrServerException | SolrException e) {
        // cluster may not be up yet
        log.error("", e);
      }
      Thread.sleep(100);
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
