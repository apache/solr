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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
public class UpdateLogCloudRollingRestartsTest extends SolrCloudTestCase {

  private static String COLLECTION;
  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 4;

  private static final Integer MAX_NUM_LOGS_TO_KEEP = 25;
  private static final Integer NUM_RECORDS_TO_KEEP = 40000;
  private static final Integer AUTO_COMMIT_MAX_TIME = 60000;
  @BeforeClass
  public static void setupCluster() throws Exception {

    System.setProperty("solr.ulog.maxNumLogsToKeep", MAX_NUM_LOGS_TO_KEEP.toString());
    System.setProperty("solr.ulog.numRecordsToKeep", NUM_RECORDS_TO_KEEP.toString());
    System.setProperty("solr.autoCommit.maxTime", AUTO_COMMIT_MAX_TIME.toString());

    // decide collection name ...
    COLLECTION = "collection"+(1+random().nextInt(100)) ;

    // create and configure cluster
    configureCluster(NUM_SHARDS*NUM_REPLICAS /* nodeCount */)
    .addConfig("conf", configset("cloud-dynamic"))
    .configure();

    // create an empty collection
    CollectionAdminRequest
    .createCollection(COLLECTION, "conf", NUM_SHARDS, NUM_REPLICAS)
    .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);
  }

  @Test
  public void test() throws Exception {

    final List<SolrClient> solrClients = new ArrayList<>();

    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      solrClients.add(jettySolrRunner.newClient());
    }

    implTest(solrClients);

    for (SolrClient solrClient : solrClients) {
      solrClient.close();
    }

  }

  private void implTest(List<SolrClient> solrClients) throws Exception {

    final List<TestThread> threads = new ArrayList<>();

    final AtomicBoolean start = new AtomicBoolean(false);
    final AtomicBoolean stop = new AtomicBoolean(false);

    // we will repeatedly poll all replicas for their current versions
    final ConcurrentLinkedQueue<Long> duplicateVersions = new ConcurrentLinkedQueue<>();
    for (SolrClient solrClient : solrClients) {
      threads.add(new ObservingTestThread(start, stop, solrClient, duplicateVersions));
    }

    // we will continuously index new documents and periodically delete old documents
    final AtomicInteger numDocs = new AtomicInteger(0);
    threads.add(new IndexingTestThread(start, stop, numDocs));
    threads.add(new DeletingTestThread(start, stop, numDocs, NUM_RECORDS_TO_KEEP/100));

    // "rolling restarts" will happen in parallel with everything else
    final ConcurrentLinkedQueue<Exception> restartExceptions = new ConcurrentLinkedQueue<>();
    threads.add(new RestartingTestThread(start, stop, restartExceptions));

    // here we go ...
    {
      for (TestThread thread : threads) {
        thread.start();
      }

      start.set(true);
      while (restartExceptions.isEmpty() && duplicateVersions.isEmpty() && numDocs.get() < NUM_RECORDS_TO_KEEP) {}
      stop.set(true);

      Collections.reverse(threads);
      for (TestThread thread : threads) {
        thread.join();
      }
    }

    // ... and finally check the results
    {
      assertTrue(restartExceptions.toString(), restartExceptions.isEmpty()); // all restarts successful

      assertTrue(duplicateVersions.toString(), duplicateVersions.isEmpty()); // no duplicate versions were observed
    }
  }

  private static abstract class TestThread extends Thread {
    private final AtomicBoolean start;
    private final AtomicBoolean stop;
    public TestThread(final AtomicBoolean start, final AtomicBoolean stop) {
      this.start = start;
      this.stop = stop;
    }
    @Override
    public void run() {
      while (!this.start.get()) {}
      while (!this.stop.get()) { doStuff(); }
    }
    abstract void doStuff();
  }

  private static final class IndexingTestThread extends TestThread {
    private final AtomicInteger numDocs;
    public IndexingTestThread(AtomicBoolean start, AtomicBoolean stop, AtomicInteger numDocs) {
      super(start, stop);
      this.numDocs = numDocs;
    }
    void doStuff() {
      try {
        final int id = numDocs.incrementAndGet();
        new UpdateRequest()
        .add(sdoc("id", "id"+id, "a_t", (id%2==0 ? "honey" : "bee"), "b_i" , id))
        .setCommitWithin(125)
        .process(cluster.getSolrClient(), COLLECTION);
      } catch (Exception ex) {
        numDocs.decrementAndGet();
      }
    }
  }

  private static final class DeletingTestThread extends TestThread {
    private final AtomicInteger numDocs;
    private final int numDocsOffset;
    public DeletingTestThread(AtomicBoolean start, AtomicBoolean stop, AtomicInteger numDocs, int numDocsOffset) {
      super(start, stop);
      this.numDocs = numDocs;
      this.numDocsOffset = numDocsOffset;
    }
    void doStuff() {
      try {
        new UpdateRequest()
        .deleteByQuery("a_t:honey AND b_i:[ * TO "+(numDocs.get()-numDocsOffset)+" ]")
        .commit(cluster.getSolrClient(), COLLECTION);
        Thread.sleep(10000);
      } catch (Exception ex) {
        // ignore
      }
    }
  }

  private static final class RestartingTestThread extends TestThread {
    protected final ConcurrentLinkedQueue<Exception> exceptions;
    public RestartingTestThread(AtomicBoolean start, AtomicBoolean stop, ConcurrentLinkedQueue<Exception> exceptions) {
      super(start, stop);
      this.exceptions = exceptions;
    }
    void doStuff() {
      // first restart the non-leaders
      for (int idx = 0; idx < cluster.getJettySolrRunners().size(); ++idx) {
        doRestart(idx, false);
      }
      // then restart the shard leader
      for (int idx = 0; idx < cluster.getJettySolrRunners().size(); ++idx) {
        if (doRestart(idx, true)) return;
      }
    }
    private boolean doRestart(int idx, boolean shardLeader) {
      final JettySolrRunner jettySolrRunner = cluster.getJettySolrRunner(idx);
      try {
        if (shardLeader == jettySolrRunner.getBaseUrl().toString().equals(
            getCollectionState(COLLECTION).getLeader("shard1").getBaseUrl())) {
          jettySolrRunner.stop();
          jettySolrRunner.start();
          AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTION, cluster.getSolrClient().getZkStateReader(), false, true, DEFAULT_TIMEOUT);
          return true;
        }
      } catch (Exception ex) {
        this.exceptions.add(ex);
      }
      return false;
    }
  }

  private static final class ObservingTestThread extends TestThread {
    private final SolrClient solrClient;
    private final ConcurrentLinkedQueue<Long> duplicateVersions;
    public ObservingTestThread(AtomicBoolean start, AtomicBoolean stop, SolrClient solrClient, ConcurrentLinkedQueue<Long> duplicateVersions) {
      super(start, stop);
      this.solrClient = solrClient;
      this.duplicateVersions = duplicateVersions;
    }
    void doStuff() {
      try {
        final QueryRequest reqV = new QueryRequest(params("qt","/get", "distrib","false", "getVersions",NUM_RECORDS_TO_KEEP.toString()));
        final NamedList<?> rspV = solrClient.request(reqV, COLLECTION);
        @SuppressWarnings("unchecked")
        final List<Long> versions = (List<Long>)rspV.get("versions");
        final Set<Long> versionSet = new HashSet<>();
        for (Long version : versions) {
          if (!versionSet.add(version)) {
            duplicateVersions.add(version);
          }
        }
      } catch (Exception ex) {
        // ignore since perhaps the instance is currently restarting
      }
    }
  }

}
