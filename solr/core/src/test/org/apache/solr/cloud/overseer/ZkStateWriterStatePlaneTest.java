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
package org.apache.solr.cloud.overseer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.StatePlaneWriter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZkStateWriterStatePlaneTest extends SolrTestCaseJ4 {
  private ZkTestServer server;
  private SolrZkClient zkClient;
  private ZkStateReader reader;

  @Before
  public void setupZk() throws Exception {
    Path zkDir = SolrTestUtil.createTempDir("zk-state-writer-state-plane");
    server = new ZkTestServer(zkDir);
    server.run();
    zkClient = new SolrZkClient(server.getZkHost(), 30000);
    zkClient.start();
    reader = new ZkStateReader(zkClient);
  }

  @After
  public void tearDownZk() throws Exception {
    if (reader != null) reader.close();
    if (zkClient != null) zkClient.close();
    if (server != null) server.shutdown();
  }

  private StatePlaneWriter.ElectionFence newElectionFence(Overseer overseer) throws Exception {
    Class<?> fenceClass = Class.forName(
        "org.apache.solr.cloud.overseer.ZkStateWriter$OverseerElectionFence");
    Constructor<?> ctor = fenceClass.getDeclaredConstructor(Overseer.class);
    ctor.setAccessible(true);
    return (StatePlaneWriter.ElectionFence) ctor.newInstance(overseer);
  }

  @Test
  public void removedCollectionIdTombstoneSurvivesWriterInstance() throws Exception {
    ZkStateWriter first = new ZkStateWriter(reader, new Stats(), null);
    Method mark = ZkStateWriter.class.getDeclaredMethod("markCollectionIdRemovedDurably", int.class);
    mark.setAccessible(true);
    mark.invoke(first, 4242);

    assertFalse("removed-id tombstones must not appear as fake collection children",
        zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE, true));

    ZkStateWriter afterFailover = new ZkStateWriter(reader, new Stats(), null);
    Method isRemoved = ZkStateWriter.class.getDeclaredMethod("isCollectionIdRemovedDurably", int.class);
    isRemoved.setAccessible(true);

    assertTrue("durable tombstone must be visible to a later overseer writer",
        (Boolean) isRemoved.invoke(afterFailover, 4242));
    assertFalse("unmarked ids must not be treated as removed",
        (Boolean) isRemoved.invoke(afterFailover, 4243));
  }

  @Test
  public void overseerElectionFenceFailsClosedWhenOwnershipCannotBeRead() throws Exception {
    assumeWorkingMockito();
    Overseer overseer = mock(Overseer.class);
    ZkController zkController = mock(ZkController.class);
    when(overseer.getElectionSeq()).thenReturn(7);
    when(overseer.getZkController()).thenReturn(zkController);
    when(overseer.isClosed()).thenReturn(false);
    when(zkController.getZkClient()).thenReturn(zkClient);

    StatePlaneWriter.ElectionFence fence = newElectionFence(overseer);

    assertFalse("production election fence must fail closed if live election ownership cannot be read",
        fence.ownsElectionAuthoritative());
  }

  @Test
  public void overseerElectionFenceAcceptsRealElectionChildFormatForOwner() throws Exception {
    assumeWorkingMockito();
    Overseer overseer = mock(Overseer.class);
    ZkController zkController = mock(ZkController.class);
    SolrZkClient zk = mock(SolrZkClient.class);
    when(overseer.getElectionSeq()).thenReturn(7);
    when(overseer.getZkController()).thenReturn(zkController);
    when(overseer.isClosed()).thenReturn(false);
    when(zkController.getZkClient()).thenReturn(zk);
    when(zk.getChildren(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null, true))
        .thenReturn(Collections.singletonList("overseer-123-n_0000000007"));

    assertTrue("production election fence must accept LeaderElector id-n_ sequence child names",
        newElectionFence(overseer).ownsElectionAuthoritative());
  }

  @Test
  public void overseerElectionFenceFailsClosedWhenElectionChildrenAreEmpty() throws Exception {
    assumeWorkingMockito();
    Overseer overseer = mock(Overseer.class);
    ZkController zkController = mock(ZkController.class);
    SolrZkClient zk = mock(SolrZkClient.class);
    when(overseer.getElectionSeq()).thenReturn(7);
    when(overseer.getZkController()).thenReturn(zkController);
    when(overseer.isClosed()).thenReturn(false);
    when(zkController.getZkClient()).thenReturn(zk);
    when(zk.getChildren(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null, true))
        .thenReturn(Collections.emptyList());

    assertFalse("production election fence must fail closed when no live election children are visible",
        newElectionFence(overseer).ownsElectionAuthoritative());
  }

  @Test
  public void overseerElectionFenceFailsClosedWhenElectionReadThrows() throws Exception {
    assumeWorkingMockito();
    Overseer overseer = mock(Overseer.class);
    ZkController zkController = mock(ZkController.class);
    SolrZkClient unreadableZkClient = mock(SolrZkClient.class);
    when(overseer.getElectionSeq()).thenReturn(7);
    when(overseer.getZkController()).thenReturn(zkController);
    when(overseer.isClosed()).thenReturn(false);
    when(zkController.getZkClient()).thenReturn(unreadableZkClient);
    when(unreadableZkClient.getChildren(anyString(), any(), anyBoolean()))
        .thenThrow(new KeeperException.ConnectionLossException());

    assertFalse("production election fence must fail closed when election read throws",
        newElectionFence(overseer).ownsElectionAuthoritative());
  }

  @Test
  public void overseerElectionFenceFailsClosedOnMalformedElectionChild() throws Exception {
    assumeWorkingMockito();
    Overseer overseer = mock(Overseer.class);
    ZkController zkController = mock(ZkController.class);
    SolrZkClient malformedZkClient = mock(SolrZkClient.class);
    when(overseer.getElectionSeq()).thenReturn(7);
    when(overseer.getZkController()).thenReturn(zkController);
    when(overseer.isClosed()).thenReturn(false);
    when(zkController.getZkClient()).thenReturn(malformedZkClient);
    when(malformedZkClient.getChildren(
            Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null, true))
        .thenReturn(Collections.singletonList("bad-child"));

    assertFalse("production election fence must fail closed on malformed election children",
        newElectionFence(overseer).ownsElectionAuthoritative());
  }

  @Test
  public void overseerElectionFenceFailsClosedWhenElectionSequenceIsMissing() throws Exception {
    assumeWorkingMockito();
    Overseer overseer = mock(Overseer.class);
    when(overseer.getElectionSeq()).thenReturn(null);
    when(overseer.isClosed()).thenReturn(false);

    assertFalse("production election fence must fail closed when the election sequence is unavailable",
        newElectionFence(overseer).ownsElectionAuthoritative());
  }

  @Test
  public void tombstonedUnknownCollectionIdCompletesWithoutPoisoningQueue() throws Exception {
    assumeWorkingMockito();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Overseer overseer = mock(Overseer.class);
      when(overseer.getElectionSeq()).thenReturn(null);
      when(overseer.getTaskZkWriterExecutor()).thenReturn(executor);

      ZkStateWriter originalWriter = new ZkStateWriter(reader, new Stats(), overseer);
      Method markRemoved =
          ZkStateWriter.class.getDeclaredMethod("markCollectionIdRemovedDurably", int.class);
      markRemoved.setAccessible(true);
      markRemoved.invoke(originalWriter, 5252);

      ZkStateWriter failoverWriter = new ZkStateWriter(reader, new Stats(), overseer);
      Method writeStateUpdates =
          ZkStateWriter.class.getDeclaredMethod("writeStateUpdatesInternal", Set.class, int.class);
      writeStateUpdates.setAccessible(true);
      @SuppressWarnings("unchecked")
      CompletableFuture<Void> future =
          (CompletableFuture<Void>) writeStateUpdates.invoke(failoverWriter, Collections.singleton(5252), 1);

      future.get(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

}
