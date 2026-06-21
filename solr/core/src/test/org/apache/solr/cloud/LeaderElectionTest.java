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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.SolrZooKeeper;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slow
@LuceneTestCase.Nightly
public class LeaderElectionTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int TIMEOUT = 30000;
  private ZkTestServer server;
  private SolrZkClient zkClient;
  private ZkStateReader zkStateReader;
  private Map<Integer,Thread> seqToThread;

  private volatile boolean stopStress = false;

  @BeforeClass
  public static void beforeLeaderElectionTest() {

  }

  @AfterClass
  public static void afterLeaderElectionTest() {

  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Path zkDir = SolrTestUtil.createTempDir("zkData");

    server = new ZkTestServer(zkDir);
    server.setTheTickTime(1000);
    server.run();

    zkClient = server.getZkClient();
    zkStateReader = new ZkStateReader(zkClient);
    seqToThread = new ConcurrentHashMap<>();
    zkClient.mkdirs("/collections/collection1");
    zkClient.mkdir("/collections/collection2");
    zkClient.mkdir("/collections/collection1/election");
    zkClient.mkdir("/collections/collection2/election");
  }

  static class TestLeaderElectionContext extends ShardLeaderElectionContextBase {
    private long runLeaderDelay = 0;

    public TestLeaderElectionContext(LeaderElector leaderElector,
        String shardId, String collection, String coreNodeName, Replica props,
        ZkController zkController, long runLeaderDelay, CoreDescriptor cd) {
      // Use the same per-shard ZK layout production uses (this fork registers the shard leader as an
      // EPHEMERAL CHILD node under the leaders path, leaderPath/<internalId>, and runs the election
      // under leader_elect/<shard>/election). The previous shared, shard-agnostic paths
      // ("/collections/<coll>/leader") meant every shard elected into one queue and wrote one leader
      // node — which broke parallel election and made getLeaderUrl read a path nothing wrote to.
      super (leaderElector,
              ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/leader_elect/" + shardId,
              ZkStateReader.getShardLeadersPath(collection, shardId), props, cd, zkController.getZkClient());
      this.runLeaderDelay = runLeaderDelay;
    }

    @Override
    boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs)
        throws KeeperException, InterruptedException, IOException {
      super.runLeaderProcess(context, weAreReplacement, pauseBeforeStartMs);
      if (runLeaderDelay > 0) {
        log.info("Sleeping for {}ms to simulate leadership takeover delay", runLeaderDelay);
        Thread.sleep(runLeaderDelay);
      }
      return true;
    }
  }

  class ElectorSetup {
    SolrZkClient zkClient;
    ZkStateReader zkStateReader;
    ZkController zkController;
    LeaderElector elector;

    public ElectorSetup(OnReconnect onReconnect) {
      // Each participant needs its OWN ZK client/session. Closing a participant must end only THIS
      // participant's session (so ZK removes its ephemeral election node and a new leader is elected),
      // WITHOUT tearing down the shared chRootClient the test uses to read leader state. The previous
      // server.getZkClient() returned the single shared chRootClient, so closing any participant closed
      // everyone's client — every getLeaderUrl after the first close then hit AlreadyClosedException.
      zkClient = new SolrZkClient(server.getZkHost(), 30000, 5000);
      zkClient.start();
      zkStateReader = new ZkStateReader(zkClient);
      zkController = MockSolrSource.makeSimpleMock(null, zkStateReader, null);
      elector = new LeaderElector(zkController);
    }

    public void close() {
      if (!zkClient.isClosed()) {
        zkClient.close();
      }
      zkStateReader.close();
    }
  }

  class ClientThread extends Thread {
    ElectorSetup es;
    private String shard;
    private String nodeName;
    private long runLeaderDelay = 0;
    private volatile int seq = -1;
    private volatile boolean stop;
    private volatile boolean electionDone = false;
    private final Replica replica;
    private volatile LeaderElector myElector;

    public ClientThread(String shard, int nodeNumber) throws Exception {
      this(null, shard, nodeNumber, 0);
    }

    public ClientThread(ElectorSetup es, String shard, int nodeNumber, long runLeaderDelay) throws Exception {
      super("Thread-" + shard + nodeNumber);
      this.shard = shard;
      this.nodeName = shard + nodeNumber;
      this.runLeaderDelay = runLeaderDelay;

      Object2ObjectMap<String,Object> props = new Object2ObjectLinkedOpenHashMap<>();
      props.put(ZkStateReader.NODE_NAME_PROP,  Integer.toString(nodeNumber));
      props.put(ZkStateReader.CORE_NAME_PROP, "");
      // Give each participant a distinct internal id == its node number. The fork names the leader's
      // ephemeral registration node leaderPath/<internalId>, so this is what getLeaderUrl reads back to
      // identify the winner; without it every replica had a null internal id and collided on one node.
      props.put("id", nodeNumber);

      replica = new Replica("", props, "", -1, null);

      this.es = es;
      if (this.es == null) {
        this.es = new ElectorSetup(new OnReconnect() {
          @Override
          public void command() throws SessionExpiredException {
            try {
              setupOnConnect();
            } catch (Throwable t) {
            }
          }

          @Override
          public String getName() {
            return "test";
          }
        });
      }
    }

    private void setupOnConnect() throws InterruptedException, KeeperException,
        IOException {
      assertNotNull(es);
      // No collection-create runs here, so create the persistent parents the per-shard election needs:
      // the election queue path (leader_elect/<shard>/election) and the leaders path under which the
      // winner registers its ephemeral leader child. Idempotent (failOnExists=false) — many threads
      // share a shard.
      es.zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/collection1/leader_elect/" + shard + LeaderElector.ELECTION_NODE, false, true);
      es.zkClient.makePath(ZkStateReader.getShardLeadersPath("collection1", shard), false, true);
      // Use a per-thread LeaderElector (sharing es's client/session, so es.close() still disconnects all
      // participants that share it at once). A single LeaderElector keeps just one `context` field, so
      // sharing es.elector across the many shards of testParallelElection let each shard's setup clobber
      // the previous one — the async election watch then ran the wrong shard's leader process and the
      // first shards never got a leader. testElection/testStressElection already give each participant its
      // own elector (es is created per-thread there), which is why they pass.
      myElector = new LeaderElector(es.zkController);
      TestLeaderElectionContext context = new TestLeaderElectionContext(myElector, shard, "collection1", nodeName, replica, es.zkController, runLeaderDelay, null);
      myElector.setup(context);
      myElector.doJoinElection(context, false, false);  // call directly, synchronous
      String seqPath = context.getLeaderSeqPath();
      if (seqPath != null) {
        seq = LeaderElector.getSeq(seqPath);
      }
      electionDone = true;
      seqToThread.put(seq, this);
    }

    @Override
    public void run() {
      try {
        setupOnConnect();
      } catch (Throwable e) {
        log.error("setup failed", e);
        es.close();
        return;
      }

      while (!stop) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
      }

    }

    public void close() {
      es.close();
      this.stop = true;
    }

    public int getSeq() {
      return seq;
    }
  }
// MRM TODO:
//  @Test
//  public void testBasic() throws Exception {
//    LeaderElector elector = new LeaderElector(zkClient);
//    ZkNodeProps props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
//        "http://127.0.0.1/solr/", ZkStateReader.CORE_NAME_PROP, "");
//    ZkController zkController = MockSolrSource.makeSimpleMock(null, null, zkClient);
//    ElectionContext context = new ShardLeaderElectionContextBase(elector,
//        "s2", "collection1", "dummynode1", props, zkController);
//    elector.setup(context);
//    elector.joinElection(context, false);
//    assertEquals("http://127.0.0.1/solr/",
//        getLeaderUrl("collection1", "s2"));
//  }

  // MRM TODO:
//  @Test
//  public void testCancelElection() throws Exception {
//    LeaderElector first = new LeaderElector(zkClient);
//    ZkNodeProps props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
//        "http://127.0.0.1/solr/", ZkStateReader.CORE_NAME_PROP, "1");
//    ZkController zkController = MockSolrSource.makeSimpleMock(null, null, zkClient);
//    ElectionContext firstContext = new ShardLeaderElectionContextBase(first,
//        "slice1", "collection2", "dummynode1", props, zkController);
//    first.setup(firstContext);
//    first.joinElection(firstContext, false);
//
//    Thread.sleep(1000);
//    assertEquals("original leader was not registered", "http://127.0.0.1/solr/1/", getLeaderUrl("collection2", "slice1"));
//
//    LeaderElector second = new LeaderElector(zkClient);
//    props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
//        "http://127.0.0.1/solr/", ZkStateReader.CORE_NAME_PROP, "2");
//    zkController = MockSolrSource.makeSimpleMock(null, null, zkClient);
//    ElectionContext context = new ShardLeaderElectionContextBase(second,
//        "slice1", "collection2", "dummynode2", props, zkController);
//    second.setup(context);
//    second.joinElection(context, false);
//    Thread.sleep(1000);
//    assertEquals("original leader should have stayed leader", "http://127.0.0.1/solr/1/", getLeaderUrl("collection2", "slice1"));
//    firstContext.cancelElection();
//    Thread.sleep(1000);
//    assertEquals("new leader was not registered", "http://127.0.0.1/solr/2/", getLeaderUrl("collection2", "slice1"));
//  }

  private String getLeaderUrl(final String collection, final String slice)
      throws KeeperException, InterruptedException {
    int iterCount = 60;
    while (iterCount-- > 0) {
      try {
        // This fork registers the shard leader as an EPHEMERAL CHILD node under the leaders path
        // (leaderPath/<internalId>) with null data, rather than writing the leader's props as JSON
        // data on the leaders znode (the upstream model this test was written for). So read the child
        // node name — it is the winner's internal id, which in this test equals its node number — and
        // return it in the "<n>/" shape getLeaderThread()/assertEquals expect.
        List<String> children = zkClient.getChildren(
            ZkStateReader.getShardLeadersPath(collection, slice), null, true);
        if (children != null && !children.isEmpty()) {
          return children.get(0) + "/";
        }
        Thread.sleep(500);
      } catch (NoNodeException | SessionExpiredException e) {
        Thread.sleep(500);
      }
    }
    throw new RuntimeException("Could not get leader props for " + collection + " " + slice);
  }

  private static void startAndJoinElection (List<ClientThread> threads) throws InterruptedException {
    for (Thread thread : threads) {
      thread.start();
    }
    int cnt = 0;
    while (true) { // wait for election to complete
      if (cnt++ == 100) {
        fail("Timeout starting and joining election");
      }
      int doneCount = 0;
      for (ClientThread thread : threads) {
        if (thread.electionDone) {
          doneCount++;
        }
      }
      if (doneCount == threads.size()) {
        break;
      }
      Thread.sleep(250);
    }
  }

  @Test
  public void testElection() throws Exception {

    List<ClientThread> threads = new ArrayList<>();

    for (int i = 0; i < 15; i++) {
      ClientThread thread = new ClientThread("shard1", i);
      threads.add(thread);
    }
    try {
      startAndJoinElection(threads);

      int leaderThread = getLeaderThread();

      // whoever the leader is, should be the n_0 seq
      assertEquals(0, threads.get(leaderThread).seq);

      // kill n_0, 1, 3 and 4
      ((ClientThread) seqToThread.get(0)).close();

      waitForLeader(threads, 1);

      leaderThread = getLeaderThread();

      // whoever the leader is, should be the n_1 seq

      assertEquals(1, threads.get(leaderThread).seq);

      ((ClientThread) seqToThread.get(4)).close();
      ((ClientThread) seqToThread.get(1)).close();
      ((ClientThread) seqToThread.get(3)).close();

      // whoever the leader is, should be the n_2 seq

      waitForLeader(threads, 2);

      leaderThread = getLeaderThread();
      assertEquals(2, threads.get(leaderThread).seq);

      // kill n_5, 2, 6, 7, and 8
      ((ClientThread) seqToThread.get(5)).close();
      ((ClientThread) seqToThread.get(2)).close();
      ((ClientThread) seqToThread.get(6)).close();
      ((ClientThread) seqToThread.get(7)).close();
      ((ClientThread) seqToThread.get(8)).close();

      waitForLeader(threads, 9);
      leaderThread = getLeaderThread();

      // whoever the leader is, should be the n_9 seq
      assertEquals(9, threads.get(leaderThread).seq);

    } finally {
      // cleanup any threads still running
      for (ClientThread thread : threads) {
        thread.close();
        thread.interrupt();

      }

      for (Thread thread : threads) {
        thread.join();
      }
    }

  }

  @Test
  public void testParallelElection() throws Exception {
    final int numShards = 2 + random().nextInt(18);
    log.info("Testing parallel election across {} shards", numShards);

    List<ClientThread> threads = new ArrayList<>();

    try {
      List<ClientThread> replica1s = new ArrayList<>();
      ElectorSetup es1 = new ElectorSetup(null);
      for (int i = 1; i <= numShards; i++) {
        ClientThread thread = new ClientThread(es1, "parshard" + i, 1, 0 /* don't delay */);
        threads.add(thread);
        replica1s.add(thread);
      }
      startAndJoinElection(replica1s);
      log.info("First replicas brought up and registered");

      // bring up second in line
      List<ClientThread> replica2s = new ArrayList<>();
      ElectorSetup es2 = new ElectorSetup(null);
      for (int i = 1; i <= numShards; i++) {
        ClientThread thread = new ClientThread(es2, "parshard" + i, 2, 40000 / (numShards - 1) /* delay enough to timeout or expire */);
        threads.add(thread);
        replica2s.add(thread);
      }
      startAndJoinElection(replica2s);
      log.info("Second replicas brought up and registered");

      // disconnect the leaders
      es1.close();

      for (int i = 1; i <= numShards; i ++) {
        // if this test fails, getLeaderUrl will more likely throw an exception and fail the test,
        // but add an assertEquals as well for good measure
        assertEquals("2/", getLeaderUrl("collection1", "parshard" + i));
      }
    } finally {
      // cleanup any threads still running
      for (ClientThread thread : threads) {
        thread.close();
        thread.interrupt();
      }
      for (Thread thread : threads) {
        thread.join();
      }
    }
  }

  private void waitForLeader(List<ClientThread> threads, int seq)
      throws KeeperException, InterruptedException {
    int leaderThread;
    int tries = 0;
    leaderThread = getLeaderThread();
    while (true) {
      final ClientThread clientThread = threads.get(leaderThread);
      if (!(clientThread.seq < seq)) break;
      leaderThread = getLeaderThread();
      if (tries++ > 50) {
        break;
      }
      Thread.sleep(200);
    }
  }

  private int getLeaderThread() throws KeeperException, InterruptedException {
    String leaderUrl = getLeaderUrl("collection1", "shard1");
    return Integer.parseInt(leaderUrl.replaceAll("/", ""));
  }

  @Test
  public void testStressElection() throws Exception {
    final ScheduledExecutorService scheduler = Executors
        .newScheduledThreadPool(3, new SolrNamedThreadFactory("stressElection"));
    final List<ClientThread> threads = Collections
        .synchronizedList(new ArrayList<ClientThread>());

    // start with a leader
    ClientThread thread1 = null;
    thread1 = new ClientThread("shard1", 0);
    threads.add(thread1);
    scheduler.schedule(thread1, 0, TimeUnit.MILLISECONDS);



    Thread scheduleThread = new Thread() {
      @Override
      public void run() {
        int count = SolrTestUtil.atLeast(5);
        for (int i = 1; i < count; i++) {
          int launchIn = random().nextInt(500);
          ClientThread thread = null;
          try {
            thread = new ClientThread("shard1", i);
          } catch (Exception e) {
            //
          }
          if (thread != null) {
            threads.add(thread);
            scheduler.schedule(thread, launchIn, TimeUnit.MILLISECONDS);
          }
        }
      }
    };

    Thread killThread = new Thread() {
      @Override
      public void run() {

        while (!stopStress) {
          try {
            int j;
            try {
              // always 1 we won't kill...
              j = random().nextInt(threads.size() - 2);
            } catch(IllegalArgumentException e) {
              continue;
            }
            try {
              threads.get(j).close();
            } catch (Exception e) {
            }

            Thread.sleep(10);
          } catch (Exception e) {
          }
        }
      }
    };

    Thread connLossThread = new Thread() {
      @Override
      public void run() {

        while (!stopStress) {
          try {
            Thread.sleep(50);
            int j;
            j = random().nextInt(threads.size());
            try {
              ((SolrZooKeeper)threads.get(j).es.zkClient.getConnectionManager().getKeeper()).closeCnxn();
              if (random().nextBoolean()) {
                long sessionId = zkClient.getSessionId();
                server.expire(sessionId);
              }
            } catch (Exception e) {
              log.error("", e);
            }
            Thread.sleep(500);

          } catch (Exception e) {

          }
        }
      }
    };

    scheduleThread.start();
    connLossThread.start();
    killThread.start();

    Thread.sleep(4000);

    stopStress = true;

    scheduleThread.interrupt();
    connLossThread.interrupt();
    killThread.interrupt();

    scheduleThread.join();
    scheduler.shutdown();

    connLossThread.join();
    killThread.join();

    int seq = threads.get(getLeaderThread()).getSeq();

    // we have a leader we know, TODO: lets check some other things

    // cleanup any threads still running
    for (ClientThread thread : threads) {
     // thread.es.zkClient.getSolrZooKeeper().close();
      thread.close();
    }

    for (Thread thread : threads) {
      thread.join();
    }


  }

  @Override
  public void tearDown() throws Exception {
    zkClient.close();
    zkStateReader.close();
    server.printLayout();
    server.shutdown();
    super.tearDown();
  }

  private void printLayout() throws Exception {
    zkClient.printLayoutToStream(System.out);
  }
}
