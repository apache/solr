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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Exercises rejoin scheduling and cancellation with real ZooKeeper election participants. */
public class ShardLeaderElectionRejoinTest extends SolrTestCase {
  private static final int WAIT_SECONDS = 30;
  private static final int REJOINS = 20;
  private static final String ELECTION_PATH = "/collections/rejoin/leader_elect/shard1/election";

  private ZkTestServer server;
  private SolrZkClient zkClient;
  private ZkStateReader zkStateReader;
  private CoreContainer cc;
  private ZkController zkController;

  @Before
  public void startZooKeeper() throws Exception {
    assumeWorkingMockito();
    server = new ZkTestServer(createTempDir("rejoin-zk"));
    server.run();
    zkClient =
        new SolrZkClient.Builder()
            .withUrl(server.getZkAddress())
            .withTimeout(WAIT_SECONDS, TimeUnit.SECONDS)
            .build();
    zkStateReader = new ZkStateReader(zkClient);
    zkClient.makePath("/collections/rejoin", true);
    cc = mock(CoreContainer.class, RETURNS_DEEP_STUBS);
    when(cc.getCore("core")).thenReturn(null);
    zkController = mock(ZkController.class);
    when(zkController.getZkClient()).thenReturn(zkClient);
    when(zkController.getZkStateReader()).thenReturn(zkStateReader);
  }

  @After
  public void stopZooKeeper() throws Exception {
    try {
      if (zkStateReader != null) {
        zkStateReader.close();
      }
    } finally {
      try {
        if (zkClient != null) {
          zkClient.close();
        }
      } finally {
        if (server != null) {
          server.shutdown();
        }
      }
    }
  }

  @Test
  public void testRepeatedRejoinDoesNotGrowStack() throws Exception {
    List<Integer> depths = new CopyOnWriteArrayList<>();
    CountDownLatch finished = new CountDownLatch(1);
    LeaderElector elector =
        new LeaderElector(zkClient) {
          @Override
          protected void runIamLeaderProcess(ElectionContext context, boolean replacement)
              throws KeeperException, InterruptedException {
            depths.add(Thread.currentThread().getStackTrace().length);
            if (depths.size() < REJOINS) {
              ((ShardLeaderElectionContext) context).rejoinLeaderElection();
            } else {
              finished.countDown();
            }
          }
        };
    ShardLeaderElectionContext context = newContext(elector);
    try {
      context.rejoinLeaderElection();
      assertTrue("Rejoins did not finish", finished.await(WAIT_SECONDS, TimeUnit.SECONDS));
      assertEquals(REJOINS, depths.size());
      int min = depths.stream().mapToInt(Integer::intValue).min().orElseThrow();
      int max = depths.stream().mapToInt(Integer::intValue).max().orElseThrow();
      assertTrue("Stack grew by " + (max - min) + " frames", max - min < REJOINS);
    } finally {
      context.close();
      context.cancelElection();
    }
    assertTrue(zkClient.getChildren(ELECTION_PATH, null).isEmpty());
  }

  @Test
  public void testCancellationWaitsForRunningRejoin() throws Exception {
    CountDownLatch joining = new CountDownLatch(1);
    CountDownLatch allowJoin = new CountDownLatch(1);
    CountDownLatch joined = new CountDownLatch(1);
    CountDownLatch cancelling = new CountDownLatch(1);
    LeaderElector elector =
        new LeaderElector(zkClient) {
          @Override
          public int joinElection(ElectionContext context, boolean replacement)
              throws KeeperException, InterruptedException {
            joining.countDown();
            if (!allowJoin.await(WAIT_SECONDS, TimeUnit.SECONDS)) {
              throw new IllegalStateException("Timed out waiting to create election node");
            }
            try {
              return super.joinElection(context, replacement);
            } finally {
              joined.countDown();
            }
          }

          @Override
          protected void runIamLeaderProcess(ElectionContext context, boolean replacement) {}
        };
    ShardLeaderElectionContext context = newContext(elector);
    ExecutorService canceller =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("cancelElection"));
    try {
      context.rejoinLeaderElection();
      assertTrue("Rejoin did not start", joining.await(WAIT_SECONDS, TimeUnit.SECONDS));
      context.close();
      Future<?> cancelled =
          canceller.submit(
              () -> {
                cancelling.countDown();
                context.cancelElection();
                return null;
              });
      assertTrue(cancelling.await(WAIT_SECONDS, TimeUnit.SECONDS));
      try {
        cancelled.get(1, TimeUnit.SECONDS);
        fail("Cancellation returned before the running rejoin could create its node");
      } catch (TimeoutException expected) {
        // Cancellation must remain blocked until the running join releases the lifecycle lock.
      } finally {
        allowJoin.countDown();
      }
      cancelled.get(WAIT_SECONDS, TimeUnit.SECONDS);
      assertTrue(joined.await(WAIT_SECONDS, TimeUnit.SECONDS));
      assertTrue(
          "Closed context left an election node",
          zkClient.getChildren(ELECTION_PATH, null).isEmpty());
    } finally {
      allowJoin.countDown();
      context.close();
      ExecutorUtil.shutdownAndAwaitTermination(canceller);
      if (joining.getCount() == 0) {
        assertTrue(joined.await(WAIT_SECONDS, TimeUnit.SECONDS));
      }
      context.cancelElection();
    }

    AtomicInteger elected = new AtomicInteger();
    LeaderElector next =
        new LeaderElector(zkClient) {
          @Override
          protected void runIamLeaderProcess(ElectionContext context, boolean replacement) {
            elected.incrementAndGet();
          }
        };
    ShardLeaderElectionContext nextContext = newContext(next);
    try {
      next.joinElection(nextContext, true);
      assertEquals("A stale participant blocked the next candidate", 1, elected.get());
    } finally {
      nextContext.close();
      nextContext.cancelElection();
    }
  }

  @Test
  public void testClosedContextDoesNotRejoin() throws Exception {
    AtomicInteger joins = new AtomicInteger();
    LeaderElector elector =
        new LeaderElector(zkClient) {
          @Override
          public int joinElection(ElectionContext context, boolean replacement) {
            joins.incrementAndGet();
            return 0;
          }
        };
    ShardLeaderElectionContext context = newContext(elector);
    context.close();
    context.rejoinLeaderElection();
    assertEquals(0, joins.get());
    assertTrue(zkClient.getChildren(ELECTION_PATH, null).isEmpty());
  }

  private ShardLeaderElectionContext newContext(LeaderElector elector) throws Exception {
    ShardLeaderElectionContext context =
        new ShardLeaderElectionContext(
            elector,
            "shard1",
            "rejoin",
            "core_node1",
            new ZkNodeProps(ZkStateReader.CORE_NAME_PROP, "core"),
            zkController,
            cc);
    elector.setup(context);
    return context;
  }
}
