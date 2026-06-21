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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@Nightly
public class RestartWhileUpdatingTest extends SolrCloudBridgeTestCase {

  private List<StoppableIndexingThread> threads;

  private volatile boolean stopExpire = false;

  @BeforeClass
  public static void beforeRestartWhileUpdatingTest() {
    sliceCount = 1;
    numJettys = 3;
    schemaString = "schema15.xml";
    System.setProperty("leaderVoteWait", "300000");
    System.setProperty("solr.autoCommit.maxTime", "30000");
    System.setProperty("solr.autoSoftCommit.maxTime", "3000");
  }

  @AfterClass
  public static void afterRestartWhileUpdatingTest() {
    System.clearProperty("leaderVoteWait");
    System.clearProperty("solr.autoCommit.maxTime");
    System.clearProperty("solr.autoSoftCommit.maxTime");
  }

  @Test
  public void test() throws Exception {
    handle.clear();
    handle.put("timestamp", SKIPVAL);

    int[] maxDocList = new int[] {5000, 10000};
    int maxDoc = maxDocList[random().nextInt(maxDocList.length - 1)];

    int numThreads = random().nextInt(4) + 1;

    threads = new ArrayList<>(numThreads);

    StoppableIndexingThread indexThread;
    ArrayList<Future> indexThreads = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      indexThread = new StoppableIndexingThread(null, cloudClient, Integer.toString(i), true, maxDoc, 1, true);
      threads.add(indexThread);
      Future<?> future = ParWork.submit("StoppableSearchThread", indexThread);
      indexThreads.add(future);
    }

    Thread.sleep(2000);

    int restartTimes = 1;
    for (int i = 0; i < restartTimes; i++) {
      Thread.sleep(random().nextInt(30000));
      stopAndStartAllReplicas();
      Thread.sleep(random().nextInt(30000));
    }

    Thread.sleep(2000);

    // stop indexing threads
    for (StoppableIndexingThread thread : threads) {
      thread.safeStop();
    }
    stopExpire = true;

    Thread.sleep(1000);

    waitForRecoveriesToFinish(COLLECTION);

    for (Future thread : indexThreads) {
      thread.get();
    }

    checkShardConsistency(false, false);
  }

  public void stopAndStartAllReplicas() throws Exception {
    List<JettySolrRunner> runners = new ArrayList<>(cluster.getJettySolrRunners());
    for (JettySolrRunner jetty : runners) {
      jetty.stop();
    }

    if (random().nextBoolean()) {
      for (StoppableIndexingThread thread : threads) {
        thread.safeStop();
      }
    }
    Thread.sleep(1000);

    for (JettySolrRunner jetty : runners) {
      jetty.start();
    }
  }

  @Override
  public void tearDown() throws Exception {
    // make sure threads have been stopped...
    if (threads != null) {
      for (StoppableIndexingThread thread : threads) {
        thread.safeStop();
      }
    }
    super.tearDown();
  }
}
