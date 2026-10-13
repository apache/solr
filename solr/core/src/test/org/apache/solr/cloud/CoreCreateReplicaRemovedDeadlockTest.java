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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.TimeOut;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes a replica from cluster state while its core is registering, so {@code
 * UnloadCoreOnDeletedWatcher} fires for a core whose pending core op is held by the creating
 * thread. The create must complete and the core must then be unloaded.
 */
public class CoreCreateReplicaRemovedDeadlockTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int COMPLETION_TIMEOUT_SEC = 45;
  private static final String WATCHER_FRAME = "UnloadCoreOnDeletedWatcher.onStateChanged";
  private static final String PENDING_OP_FRAME = "SolrCores.waitAddPendingCoreOps";

  private static final AtomicBoolean ARMED = new AtomicBoolean(false);
  private static volatile String targetCollection;
  private static volatile CountDownLatch reached;
  private static volatile CountDownLatch release;
  private static volatile String blockedCoreName;
  private static volatile String blockedCoreNodeName;
  private static volatile long createThreadId;

  /**
   * Pauses the first {@code recoverFromLog()} call on a core of {@link #targetCollection} once
   * armed. {@code ZkController.register} calls it after confirming the replica is in cluster state
   * and before installing {@code UnloadCoreOnDeletedWatcher}.
   */
  public static class BlockingUpdateLog extends UpdateLog {
    private String coreName;
    private String coreNodeName;
    private String collection;

    @Override
    public void init(UpdateHandler uhandler, SolrCore core) {
      super.init(uhandler, core);
      coreName = core.getName();
      var cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
      if (cloudDesc != null) {
        collection = cloudDesc.getCollectionName();
        coreNodeName = cloudDesc.getCoreNodeName();
      }
    }

    @Override
    public Future<RecoveryInfo> recoverFromLog() {
      if (collection != null
          && collection.equals(targetCollection)
          && ARMED.compareAndSet(true, false)) {
        blockedCoreName = coreName;
        blockedCoreNodeName = coreNodeName;
        createThreadId = Thread.currentThread().threadId();
        log.info("Pausing register of core {} ({}) in recoverFromLog", coreName, coreNodeName);
        reached.countDown();
        try {
          if (!release.await(6, TimeUnit.MINUTES)) {
            log.warn("Never released, continuing");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        log.info("Resuming register of core {}", coreName);
      }
      return super.recoverFromLog();
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.ulog", BlockingUpdateLog.class.getName());
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Before
  public void resetHook() {
    ARMED.set(false);
    reached = new CountDownLatch(1);
    release = new CountDownLatch(1);
    blockedCoreName = null;
    blockedCoreNodeName = null;
    createThreadId = -1;
  }

  /**
   * Node B already hosts a replica of the collection, so its ZkStateReader already watches it and
   * {@code registerDocCollectionWatcher} evaluates the new watcher only inline, on the creating
   * thread.
   */
  @Test
  public void testCollectionAlreadyWatched() throws Exception {
    JettySolrRunner nodeA = cluster.getJettySolrRunner(0);
    JettySolrRunner nodeB = cluster.getJettySolrRunner(1);
    String collection = "watchedColl";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 2)
        .setCreateNodeSet(nodeA.getNodeName() + "," + nodeB.getNodeName())
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 2);

    runScenario(collection, nodeB);
  }

  /**
   * First replica of the collection on node B: {@code registerDocCollectionWatcher} also starts a
   * new ZK watch and queues an async notification, so the inline call races the notification thread
   * on the same watcher.
   */
  @Test
  public void testFirstReplicaOnNode() throws Exception {
    JettySolrRunner nodeA = cluster.getJettySolrRunner(0);
    JettySolrRunner nodeB = cluster.getJettySolrRunner(1);
    String collection = "unwatchedColl";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .setCreateNodeSet(nodeA.getNodeName())
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);

    runScenario(collection, nodeB);
  }

  private void runScenario(String collection, JettySolrRunner nodeB) throws Exception {
    targetCollection = collection;
    ARMED.set(true);
    // async, since ADDREPLICA keeps waiting for the removed replica to show up in cluster state
    CollectionAdminRequest.addReplicaToShard(collection, "shard1")
        .setNode(nodeB.getNodeName())
        .processAsync(cluster.getSolrClient());
    try {
      assertTrue(
          "Core create on node B never reached recoverFromLog",
          reached.await(60, TimeUnit.SECONDS));
      log.info(
          "Core {} ({}) paused inside ZkController.register, removing replica from state.json",
          blockedCoreName,
          blockedCoreNodeName);
      removeReplicaFromClusterState(nodeB, collection);
      cluster
          .getZkStateReader()
          .waitForState(
              collection,
              30,
              TimeUnit.SECONDS,
              c -> c != null && c.getReplica(blockedCoreNodeName) == null);
    } finally {
      release.countDown();
    }

    try {
      new TimeOut(COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS, TimeSource.NANO_TIME)
          .waitFor("core create still running", () -> !createThreadInCreate());
    } catch (TimeoutException e) {
      failWithDiagnostics();
    }

    CoreContainer cc = nodeB.getCoreContainer();
    new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME)
        .waitFor(
            "Core " + blockedCoreName + " was not unloaded on node B",
            () ->
                cc.getCoreDescriptor(blockedCoreName) == null
                    && !cc.getAllCoreNames().contains(blockedCoreName));
    assertEquals("Threads waiting on a pending core op", List.of(), pendingOpWaiters());
  }

  private static boolean createThreadInCreate() {
    ThreadInfo ti =
        ManagementFactory.getThreadMXBean().getThreadInfo(createThreadId, Integer.MAX_VALUE);
    return ti != null && formatStack(ti.getStackTrace()).contains("CoreContainer.create(");
  }

  /** Removes the replica from state.json directly, as DELETEREPLICA eventually would. */
  private void removeReplicaFromClusterState(JettySolrRunner node, String collection)
      throws Exception {
    ZkController zkController = node.getCoreContainer().getZkController();
    ZkNodeProps m =
        new ZkNodeProps(
            Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
            ZkStateReader.CORE_NAME_PROP, blockedCoreName,
            ZkStateReader.NODE_NAME_PROP, node.getNodeName(),
            ZkStateReader.COLLECTION_PROP, collection,
            ZkStateReader.CORE_NODE_NAME_PROP, blockedCoreNodeName);
    if (zkController.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
      zkController
          .getDistributedClusterStateUpdater()
          .doSingleStateUpdate(
              DistributedClusterStateUpdater.MutatingCommand.SliceRemoveReplica,
              m,
              zkController.getSolrCloudManager(),
              zkController.getZkStateReader());
    } else {
      cluster.getOpenOverseer().getStateUpdateQueue().offer(m);
    }
  }

  private static List<String> pendingOpWaiters() {
    List<String> waiters = new ArrayList<>();
    for (ThreadInfo ti : ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)) {
      if (ti.getThreadState() == Thread.State.WAITING
          && formatStack(ti.getStackTrace()).contains(PENDING_OP_FRAME)) {
        waiters.add(header(ti) + solrFrames(ti.getStackTrace()));
      }
    }
    return waiters;
  }

  private void failWithDiagnostics() {
    ThreadInfo[] infos = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
    List<String> involved = new ArrayList<>();
    String createThread = null;
    for (ThreadInfo ti : infos) {
      boolean isCreateThread = ti.getThreadId() == createThreadId;
      boolean isWatcherForCore =
          ti.getThreadName().contains(blockedCoreName)
              && formatStack(ti.getStackTrace()).contains(WATCHER_FRAME);
      if (!isCreateThread && !isWatcherForCore) {
        continue;
      }
      String entry = header(ti) + solrFrames(ti.getStackTrace());
      if (isCreateThread) {
        createThread = entry;
      } else {
        involved.add(entry);
      }
      log.error("Thread involved in deadlock on core {}:\n{}", blockedCoreName, entry);
    }
    fail(
        "Core create did not complete within "
            + COMPLETION_TIMEOUT_SEC
            + "s. Create thread for core "
            + blockedCoreName
            + ":\n"
            + createThread
            + "Other threads in "
            + WATCHER_FRAME
            + " for this core: "
            + involved.size()
            + "\n"
            + String.join("\n", involved));
  }

  private static String header(ThreadInfo ti) {
    StringBuilder sb = new StringBuilder();
    sb.append('"').append(ti.getThreadName()).append("\" ").append(ti.getThreadState());
    if (ti.getLockName() != null) {
      sb.append(" on ").append(ti.getLockName());
    }
    if (ti.getLockOwnerName() != null) {
      sb.append(" owned by \"").append(ti.getLockOwnerName()).append('"');
    }
    return sb.append('\n').toString();
  }

  private static String solrFrames(StackTraceElement[] frames) {
    StringBuilder sb = new StringBuilder();
    for (StackTraceElement f : frames) {
      if (f.getClassName().startsWith("org.apache.solr.")) {
        sb.append("\tat ").append(f).append('\n');
      }
    }
    return sb.toString();
  }

  private static String formatStack(StackTraceElement[] frames) {
    StringBuilder sb = new StringBuilder();
    for (StackTraceElement f : frames) {
      sb.append("\tat ").append(f).append('\n');
    }
    return sb.toString();
  }
}
