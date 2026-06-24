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

  private void markRemoved(ZkStateWriter writer, int collId) throws Exception {
    Method mark = ZkStateWriter.class.getDeclaredMethod("markCollectionIdRemovedDurably", int.class);
    mark.setAccessible(true);
    invoke(mark, writer, collId);
  }

  private boolean isRemoved(ZkStateWriter writer, int collId) throws Exception {
    Method isRemoved = ZkStateWriter.class.getDeclaredMethod("isCollectionIdRemovedDurably", int.class);
    isRemoved.setAccessible(true);
    return (Boolean) invoke(isRemoved, writer, collId);
  }

  private Object invoke(Method method, Object target, Object... args) throws Exception {
    try {
      return method.invoke(target, args);
    } catch (java.lang.reflect.InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  private org.apache.solr.common.cloud.DocCollection emptyCollection(String name, int id) {
    java.util.Map<String,Object> props = new java.util.HashMap<>();
    props.put("id", id);
    return new org.apache.solr.common.cloud.DocCollection(name, Collections.emptyMap(), props,
        org.apache.solr.common.cloud.CompositeIdRouter.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  private java.util.Map<String,org.apache.solr.common.cloud.DocCollection> collectionMap(
      ZkStateWriter writer) throws Exception {
    java.lang.reflect.Field field = ZkStateWriter.class.getDeclaredField("cs");
    field.setAccessible(true);
    return (java.util.Map<String,org.apache.solr.common.cloud.DocCollection>) field.get(writer);
  }

  @SuppressWarnings("unchecked")
  private java.util.Map<Integer,String> idToCollectionMap(ZkStateWriter writer) throws Exception {
    java.lang.reflect.Field field = ZkStateWriter.class.getDeclaredField("idToCollection");
    field.setAccessible(true);
    return (java.util.Map<Integer,String>) field.get(writer);
  }

  @Test
  public void removedCollectionIdTombstoneSurvivesWriterInstance() throws Exception {
    ZkStateWriter first = new ZkStateWriter(reader, new Stats(), null);
    markRemoved(first, 4242);

    java.util.List<String> tombstoneChildren =
        zkClient.getChildren("/overseer/stateplane_removed_ids", null, true);
    assertFalse("new writes must not create one persistent znode per removed id",
        tombstoneChildren.contains("4242"));
    assertTrue("new writes must use bounded sharded tombstone buckets",
        tombstoneChildren.stream().anyMatch(child -> child.startsWith("bucket-")));

    ZkStateWriter afterFailover = new ZkStateWriter(reader, new Stats(), null);

    assertTrue("durable tombstone must be visible to a later overseer writer",
        isRemoved(afterFailover, 4242));
    assertFalse("unmarked ids must not be treated as removed",
        isRemoved(afterFailover, 4243));
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


  @Test
  public void removeCollectionFailsClosedWhenDurableTombstoneCannotBePersisted() throws Exception {
    String bucketCountProp = "solr.zkStateWriter.removedCollIdTombstoneBuckets";
    String bucketCapProp = "solr.zkStateWriter.removedCollIdTombstoneBucketEntryCap";
    String oldBucketCount = System.getProperty(bucketCountProp);
    String oldBucketCap = System.getProperty(bucketCapProp);
    try {
      System.setProperty(bucketCountProp, "1");
      System.setProperty(bucketCapProp, "1");

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats(), null);
      markRemoved(writer, 7000);
      collectionMap(writer).put("deleteMe", emptyCollection("deleteMe", 7001));
      idToCollectionMap(writer).put(7001, "deleteMe");

      org.apache.lucene.util.LuceneTestCase.expectThrows(org.apache.solr.common.SolrException.class,
          () -> writer.removeCollection("deleteMe"));

      assertTrue("collection structure must remain armed when tombstone persistence fails",
          collectionMap(writer).containsKey("deleteMe"));
      assertEquals("deleteMe", idToCollectionMap(writer).get(7001));
      assertFalse("failed tombstone must not be treated as durable removal", isRemoved(writer, 7001));
    } finally {
      if (oldBucketCount == null) System.clearProperty(bucketCountProp);
      else System.setProperty(bucketCountProp, oldBucketCount);
      if (oldBucketCap == null) System.clearProperty(bucketCapProp);
      else System.setProperty(bucketCapProp, oldBucketCap);
    }
  }

  @Test
  public void removedCollectionIdTombstoneBucketFailsClosedAtCap() throws Exception {
    String bucketCountProp = "solr.zkStateWriter.removedCollIdTombstoneBuckets";
    String bucketCapProp = "solr.zkStateWriter.removedCollIdTombstoneBucketEntryCap";
    String oldBucketCount = System.getProperty(bucketCountProp);
    String oldBucketCap = System.getProperty(bucketCapProp);
    try {
      System.setProperty(bucketCountProp, "1");
      System.setProperty(bucketCapProp, "1");

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats(), null);
      markRemoved(writer, 7100);

      org.apache.lucene.util.LuceneTestCase.expectThrows(org.apache.solr.common.SolrException.class,
          () -> markRemoved(writer, 7101));
      assertTrue(isRemoved(writer, 7100));
      assertFalse("bucket overflow must fail closed rather than evicting old tombstones",
          isRemoved(writer, 7101));
    } finally {
      if (oldBucketCount == null) System.clearProperty(bucketCountProp);
      else System.setProperty(bucketCountProp, oldBucketCount);
      if (oldBucketCap == null) System.clearProperty(bucketCapProp);
      else System.setProperty(bucketCapProp, oldBucketCap);
    }
  }

  @Test
  public void collectionIdAllocationSkipsDurablyRemovedIds() throws Exception {
    ZkStateWriter writer = new ZkStateWriter(reader, new Stats(), null);
    markRemoved(writer, 1);

    assertEquals("new collection ids must not reuse durable removed ids",
        Integer.valueOf(2), writer.getHighestId("newCollection"));
    assertEquals("newCollection", idToCollectionMap(writer).get(2));
    assertFalse("skipped tombstone id must not be assigned to the new collection",
        idToCollectionMap(writer).containsKey(1));
  }

  @Test
  public void removedCollectionIdBucketCasRetriesAfterRealBadVersion() throws Exception {
    String bucketCountProp = "solr.zkStateWriter.removedCollIdTombstoneBuckets";
    String oldBucketCount = System.getProperty(bucketCountProp);
    try {
      System.setProperty(bucketCountProp, "1");
      String bucketPath = "/overseer/stateplane_removed_ids/bucket-000";
      zkClient.makePath(bucketPath, "8000\n".getBytes(java.nio.charset.StandardCharsets.UTF_8),
          org.apache.zookeeper.CreateMode.PERSISTENT, null, false, true);

      SolrZkClient spyZkClient = org.mockito.Mockito.spy(zkClient);
      ZkStateReader spyReader = new ZkStateReader(spyZkClient);
      java.util.concurrent.atomic.AtomicBoolean mutateAfterFirstRead =
          new java.util.concurrent.atomic.AtomicBoolean(true);
      org.mockito.Mockito.doAnswer(invocation -> {
        byte[] data = (byte[]) invocation.callRealMethod();
        if (mutateAfterFirstRead.getAndSet(false)) {
          zkClient.setData(bucketPath,
              "8000\n8999\n".getBytes(java.nio.charset.StandardCharsets.UTF_8), -1, true);
        }
        return data;
      }).when(spyZkClient).getData(
          org.mockito.ArgumentMatchers.eq(bucketPath),
          org.mockito.ArgumentMatchers.isNull(),
          org.mockito.ArgumentMatchers.any(org.apache.zookeeper.data.Stat.class),
          org.mockito.ArgumentMatchers.eq(true));

      ZkStateWriter writer = new ZkStateWriter(spyReader, new Stats(), null);
      markRemoved(writer, 8001);

      byte[] data = zkClient.getData(bucketPath, null, (org.apache.zookeeper.data.Stat) null, true);
      String bucket = new String(data, java.nio.charset.StandardCharsets.UTF_8);
      assertTrue(bucket.contains("8000\n"));
      assertTrue(bucket.contains("8999\n"));
      assertTrue("CAS retry must preserve and add the requested tombstone after BadVersion",
          bucket.contains("8001\n"));
    } finally {
      if (oldBucketCount == null) System.clearProperty(bucketCountProp);
      else System.setProperty(bucketCountProp, oldBucketCount);
    }
  }

}
