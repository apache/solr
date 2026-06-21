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

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.overseer.CollectionWorkQueueWatcher;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.QueueWatcher;
import org.apache.solr.cloud.overseer.WorkQueueWatcher;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.</p>
 *
 * <p>The <b>Overseer</b> is a single elected node in the SolrCloud cluster that is in charge of interactions with
 * ZooKeeper that require global synchronization. </p>
 *
 * <p>The Overseer deals with:</p>
 * <ul>
 *   <li>Cluster State updates, i.e. updating Collections' <code>state.json</code> files in ZooKeeper,</li>
 *   <li>Collection API implementation, see
 *   {@link OverseerCollectionConfigSetProcessor} and {@link OverseerCollectionMessageHandler} (and the example below),</li>
 *   <li>Updating Config Sets, see {@link OverseerCollectionConfigSetProcessor} and {@link OverseerConfigSetMessageHandler},</li>
 * </ul>
 *
 * <p>The nodes in the cluster communicate with the Overseer over queues implemented in ZooKeeper. There are essentially
 * two queues:</p>
 * <ol>
 *   <li>The <b>state update queue</b>, through which nodes request the Overseer to update the <code>state.json</code> file of a
 *   Collection in ZooKeeper. This queue is in Zookeeper at <code>/overseer/queue</code>,</li>
 *   <li>A queue shared between <b>Collection API and Config Set API</b> requests. This queue is in Zookeeper at
 *   <code>/overseer/collection-queue-work</code>.</li>
 * </ol>
 *
 * <p>An example of the steps involved in the Overseer processing a Collection creation API call:</p>
 * <ol>
 *   <li>Client uses the Collection API with <code>CREATE</code> action and reaches a node of the cluster,</li>
 *   <li>The node (via {@link CollectionsHandler}) enqueues the request into the <code>/overseer/collection-queue-work</code>
 *   queue in ZooKeepeer,</li>
 *   <li>The {@link OverseerCollectionConfigSetProcessor} running on the Overseer node dequeues the message and using an
 *   executor service with a maximum pool size of {@link OverseerTaskProcessor#MAX_PARALLEL_TASKS} hands it for processing
 *   to {@link OverseerCollectionMessageHandler},</li>
 *   <li>Command {@link CreateCollectionCmd} then executes and does:
 *   <ol>
 *     <li>Update some state directly in ZooKeeper (creating collection znode),</li>
 *     <li>Compute replica placement on available nodes in the cluster,</li>
 *     <li>Enqueue a state change request for creating the <code>state.json</code> file for the collection in ZooKeeper.
 *     This is done by enqueuing a message in <code>/overseer/queue</code>,</li>
 *     <li>The command then waits for the update to be seen in ZooKeeper...</li>
 *   </ol></li>
 *   <li>The ClusterState Updater (also running on the Overseer node) dequeues the state change message and creates the
 *   <code>state.json</code> file in ZooKeeper for the Collection. All the work of the cluster state updater
 *   (creations, updates, deletes) is done sequentially for the whole cluster by a single thread.</li>
 *   <li>The {@link CreateCollectionCmd} sees the state change in
 *   ZooKeeper and:
 *   <ol start="5">
 *     <li>Builds and sends requests to each node to create the appropriate cores for all the replicas of all shards
 *     of the collection. Nodes create the replicas and set them to {@link org.apache.solr.common.cloud.Replica.State#ACTIVE}.</li>
 *   </ol></li>
 *   <li>The collection creation command has succeeded from the Overseer perspective,</li>
 *   <li>{@link CollectionsHandler} checks the replicas in Zookeeper and verifies they are all
 *   {@link org.apache.solr.common.cloud.Replica.State#ACTIVE},</li>
 *   <li>The client receives a success return.</li>
 * </ol>
 */
public class Overseer implements SolrCloseable {
  public static final String QUEUE_OPERATION = "op";

  public static final String OVERSEER_COLLECTION_QUEUE_WORK = "/overseer/collection-queue-work";

  public static final String OVERSEER_QUEUE = "/overseer/queue";

  public static final String OVERSEER_ASYNC_IDS = "/overseer/async_ids";

  public static final String OVERSEER_COLLECTION_MAP_FAILURE = "/overseer/collection-map-failure";

  public static final String OVERSEER_COLLECTION_MAP_COMPLETED = "/overseer/collection-map-completed";

  public static final String OVERSEER_COLLECTION_MAP_RUNNING = "/overseer/collection-map-running";


  public static final int STATE_UPDATE_MAX_QUEUE = 20000;

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  public static final String OVERSEER_ELECT = "/overseer/overseer_elect";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile boolean closeAndDone;

  private volatile QueueWatcher queueWatcher;
  private volatile CollectionWorkQueueWatcher collectionQueueWatcher;
  private volatile ExecutorService taskExecutor;
  private volatile ExecutorService collectionQueueExecutor;

  private volatile ExecutorService zkWriterExecutor;

  private final ReentrantLock startLock = new ReentrantLock();

  public boolean isDone() {
    return closeAndDone;
  }

  public ExecutorService getTaskExecutor() {
    return taskExecutor;
  }

  public ExecutorService getCollectionQueueTaskExecutor() {
    return collectionQueueExecutor;
  }

  public ExecutorService getTaskZkWriterExecutor() {
    return zkWriterExecutor;
  }

 // public static class OverseerThread extends SolrThread implements Closeable {
 public static class OverseerThread extends Thread implements Closeable {

    protected volatile boolean isClosed;
    private final Closeable thread;

    public OverseerThread(ThreadGroup ccTg, Closeable thread, String name) {
      super(ccTg, (Runnable) thread, name);
      this.thread = thread;
    }

    @Override
    public void run() {
      super.run();
    }

    @Override
    public void close() throws IOException {
      this.isClosed = true;
      thread.close();
    }

    public Closeable getThread() {
      return thread;
    }

    public boolean isClosed() {
      return this.isClosed;
    }

  }


 // private volatile OverseerThread updaterThread;

//  private volatile ExecutorService stateManagmentExecutor;
//
//  private volatile ExecutorService taskExecutor;

  private final ZkStateWriter zkStateWriter;

  private final UpdateShardHandler updateShardHandler;

  private final String adminPath;

  private final ZkController zkController;

  private volatile Stats stats;
  private volatile Integer id;
  // Monotonic ZK election sequence of this overseer's winning candidate node (NOT the port-based
  // {@link #id}). Used as the delta-plane writer-fence id so a stale prior overseer cannot out-rank a
  // legitimately newer one. Ports are not ordered by election recency; the ZK sequence is.
  private volatile Integer electionSeq;
  private volatile boolean closed = true;

  private final CloudConfig config;

  public volatile Http2SolrClient overseerOnlyClient;
  public volatile LBHttp2SolrClient overseerLbClient;


  // overseer not responsible for closing reader
  public Overseer(UpdateShardHandler updateShardHandler, String adminPath, ZkController zkController, CloudConfig config) {
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;

    Stats stats = new Stats();
    this.zkStateWriter = new ZkStateWriter(zkController.getZkStateReader(), stats, this);
  }

  public void start(Integer id, ElectionContext context, boolean weAreReplacement) throws KeeperException {
    startLock.lock();
    try {
      log.info("Starting Overseer");
      if (getCoreContainer().isShutDown() || closeAndDone) {
        if (log.isDebugEnabled()) log.debug("Already closed, exiting");
        return;
      }

      closed = false;

      this.id = id;
      Integer seq = null;
      if (context != null && context.getLeaderSeqPath() != null) {
        try {
          seq = LeaderElector.getSeq(context.getLeaderSeqPath());
        } catch (RuntimeException e) {
          // Malformed/absent seq path (minimal test setups) — fence degrades to disabled (null), safe.
          log.warn("Could not derive overseer election sequence from {}; delta-plane writer fence disabled",
              context.getLeaderSeqPath(), e);
        }
      }
      this.electionSeq = seq;
      //getParExecutorService(String name, int corePoolSize, int maxPoolSize, int keepAliveTime, BlockingQueue queue)
      //     taskExecutor = ParWork.getExecutorService("OverseertaskExecutor", 64);
      taskExecutor = ParWork.getExecutorService("OverseertaskExecutor", 32, false);

      collectionQueueExecutor = ParWork.getParExecutorService("collectionQueueExecutor", 4, 16, 1000);
          // ParWork.getExecutorService("OverseerCollectionQueueTaskExecutor", 128, false);

      zkWriterExecutor = ParWork.getParExecutorService("zkWriterExecutor", 4, 16, 1000);

          //ParWork.getExecutorService("zkWriterExecutor",32, false);

     // zkWriterExecutor = (ThreadPoolExecutor) ParWork.getParExecutorService("zkWriterExecutor", 16, 128, 3000, new LinkedBlockingDeque());
     // zkWriterExecutor.prestartAllCoreThreads();
      overseerOnlyClient = new Http2SolrClient.Builder().markInternalRequest().maxThreadPoolSize(128).idleTimeout(600000).connectionTimeout(5000).build();
      overseerOnlyClient.enableCloseLock();

      this.overseerLbClient = new LBHttp2SolrClient(overseerOnlyClient, true);

      //    try {
      //      if (log.isDebugEnabled()) {
      //        log.debug("set watch on leader znode");
      //      }
      //      zkController.getZkClient().exists(Overseer.OVERSEER_ELECT + "/leader", new Watcher() {
      //
      //        @Override
      //        public void process(WatchedEvent event) {
      //          if (Event.EventType.None.equals(event.getType())) {
      //            return;
      //          }
      //          if (!isClosed()) {
      //            log.info("Overseer leader has changed, closing ...");
      //            Overseer.this.close();
      //          }
      //        }}, true);
      //    } catch (KeeperException.SessionExpiredException e) {
      //      log.warn("ZooKeeper session expired");
      //      return;
      //    } catch (InterruptedException | AlreadyClosedException e) {
      //      log.info("Already closed");
      //      return;
      //    } catch (Exception e) {
      //      log.error("Unexpected error in Overseer state update loop", e);
      //      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      //    }

      log.info("Overseer (id={}) starting", id);
      //launch cluster state updater thread

      ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");

      this.zkStateWriter.init(weAreReplacement);

      queueWatcher = new WorkQueueWatcher(this, getCoreContainer());
      collectionQueueWatcher = new CollectionWorkQueueWatcher(getCoreContainer(), id, overseerLbClient, adminPath, stats, Overseer.this);
      try {
        queueWatcher.start(weAreReplacement);
        collectionQueueWatcher.start(weAreReplacement);
      } catch (InterruptedException e) {
        log.warn("interrupted", e);
      }

      // TODO: don't track for a moment, can leak out of collection api tests
      // assert ObjectReleaseTracker.track(this);
    } finally {
      startLock.unlock();
    }
  }

  public Integer getId() {
    return id;
  }

  /**
   * Monotonic ZK election sequence of this overseer's winning candidate (null in minimal setups with
   * no election path). This — not {@link #getId()}, which is the non-monotonic node port — is the
   * delta-plane writer-fence identity.
   */
  public Integer getElectionSeq() {
    return electionSeq;
  }

  public Stats getStats() {
    return stats;
  }

  public ZkController getZkController(){
    return zkController;
  }

  public CoreContainer getCoreContainer() {
    return zkController.getCoreContainer();
  }

  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }


  public void closeAndDone() {
    startLock.lock();
    try {
      this.closed = true;
      this.closeAndDone = true;
    } finally {
      startLock.unlock();
    }
     close();
  }

  public boolean isCloseAndDone() {
    return closeAndDone;
  }

  public void close() {
    log.info("Overseer (id={}) closing closeAndDone={}}", id, closeAndDone);

    boolean cd = closeAndDone;

    closed = true;

    try {
      zkStateWriter.stop();
    } catch (InterruptedException e) {
      log.debug("ZkStateWriter interrupted on stop", e);
    }

    if (collectionQueueExecutor != null) {
      collectionQueueExecutor.shutdown();
    }

    if (taskExecutor != null) {
      taskExecutor.shutdown();
    }

    if (zkWriterExecutor != null) {
      zkWriterExecutor.shutdown();
    }


    if (collectionQueueExecutor != null) {
      try {
        collectionQueueExecutor.awaitTermination(15, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e, true);
      }
    }


    if (taskExecutor != null) {
      try {
        taskExecutor.awaitTermination(15, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e, true);
      }
    }

    if (zkWriterExecutor != null) {
      try {
        zkWriterExecutor.awaitTermination(15, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e, true);
      }
    }

    if (overseerLbClient != null) {
      overseerLbClient.close();
      overseerLbClient = null;
    }


    if (overseerOnlyClient != null) {
      overseerOnlyClient.disableCloseLock();
      overseerOnlyClient.close();
      overseerOnlyClient = null;
    }

    IOUtils.closeQuietly(queueWatcher);
    IOUtils.closeQuietly(collectionQueueWatcher);

    if (!cd) {
      boolean retry;
      synchronized (this) {
        retry = !zkController.getCoreContainer().isShutDown() && !zkController.isShutdownCalled() && !zkController.isClosed() && !closeAndDone;
      }
      if (retry && zkController.getZkClient().isAlive()) {
        log.info("rejoining the overseer election after closing");
        try {
          zkController.rejoinOverseerElection(false);
        } catch (AlreadyClosedException e) {

        } catch (Exception e) {
          log.warn("Could not rejoin election", e);
        }
      }

    }

    if (log.isDebugEnabled()) {
      log.debug("doClose - end");
    }

    assert ObjectReleaseTracker.getInstance().release(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }


  /**
   * Get queue that can be used to send messages to Overseer.
   * <p>
   * Any and all modifications to the cluster state must be sent to
   * the overseer via this queue. The complete list of overseer actions
   * supported by this queue are documented inside the {@link OverseerAction} enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   * Therefore, this method should be used only by clients for writing to the overseer queue.
   * <p>
   *
   * @return a {@link ZkDistributedQueue} object
   */
  public ZkDistributedQueue getStateUpdateQueue() {
    return getStateUpdateQueue(new Stats());
  }

  /**
   * The overseer uses the returned queue to read any operations submitted by clients.
   * This method should not be used directly by anyone other than the Overseer itself.
   * This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkStats  a {@link Stats} object which tracks statistics for all zookeeper operations performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  ZkDistributedQueue getStateUpdateQueue(Stats zkStats) {
    return new ZkDistributedQueue(zkController.getZkClient(), "/overseer/queue", zkStats, STATE_UPDATE_MAX_QUEUE, new ConnectionManager.IsClosed(){
      public boolean isClosed() {
        return Overseer.this.isClosed() || zkController.getCoreContainer().isShutDown();
      }
    });
  }

//  static ZkDistributedQueue getInternalWorkQueue(final SolrZkClient zkClient, Stats zkStats) {
//    return new ZkDistributedQueue(zkClient, "/overseer/queue-work", zkStats);
//  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  public static DistributedMap getRunningMap(final SolrZkClient zkClient) throws KeeperException {
    return new DistributedMap(zkClient, "/overseer/collection-map-running");
  }

  /* Size-limited map for successfully completed tasks*/
  public static DistributedMap getCompletedMap(final SolrZkClient zkClient) throws KeeperException {
    return new DistributedMap(zkClient, "/overseer/collection-map-completed");
  }

  /* Map for failed tasks, not to be used outside of the Overseer */
  public static DistributedMap getFailureMap(final SolrZkClient zkClient) throws KeeperException {
    return new DistributedMap(zkClient, "/overseer/collection-map-failure");
  }

  /* Map of async IDs currently in use*/
  static DistributedMap getAsyncIdsMap(final SolrZkClient zkClient) throws KeeperException {
    return new DistributedMap(zkClient, Overseer.OVERSEER_ASYNC_IDS);
  }

  /**
   * Get queue that can be used to submit collection API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link CollectionsHandler} to submit collection API
   * tasks which are executed by the {@link OverseerCollectionMessageHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  static OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient) {
    return getCollectionQueue(zkClient, new Stats());
  }

  /**
   * Get queue that can be used to read collection API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link OverseerCollectionMessageHandler} to read collection API
   * tasks submitted by the {@link CollectionsHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue are tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  static OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient, Stats zkStats) {
    return new OverseerTaskQueue(zkClient, "/overseer/collection-queue-work", zkStats);
  }

  /**
   * Get queue that can be used to submit configset API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link org.apache.solr.handler.admin.ConfigSetsHandler} to submit
   * tasks which are executed by the {@link OverseerConfigSetMessageHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.ConfigSetParams.ConfigSetAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue
   * are <em>not</em> tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  static OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient)  {
    return getConfigSetQueue(zkClient, new Stats());
  }

  /**
   * Get queue that can be used to read configset API tasks to the Overseer.
   * <p>
   * This queue is used internally by the {@link OverseerConfigSetMessageHandler} to read configset API
   * tasks submitted by the {@link org.apache.solr.handler.admin.ConfigSetsHandler}. The actions supported
   * by this queue are listed in the {@link org.apache.solr.common.params.ConfigSetParams.ConfigSetAction}
   * enum.
   * <p>
   * Performance statistics on the returned queue are tracked by the Overseer Stats API,
   * see {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   * <p>
   * For now, this internally returns the same queue as {@link #getCollectionQueue(SolrZkClient, Stats)}.
   * It is the responsibility of the client to ensure that configset API actions are prefixed with
   * {@link OverseerConfigSetMessageHandler#CONFIGSETS_ACTION_PREFIX} so that it is processed by
   * {@link OverseerConfigSetMessageHandler}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  static OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient, Stats zkStats) {
    // For now, we use the same queue as the collection queue, but ensure
    // that the actions are prefixed with a unique string.
    return getCollectionQueue(zkClient, zkStats);
  }

  public ZkStateReader getZkStateReader() {
    return zkController.getZkStateReader();
  }

  public ZkStateWriter getZkStateWriter() {
    return zkStateWriter;
  }

  public void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    getStateUpdateQueue().offer(data, false);
  }

  public Future writePendingUpdates(String collection) {
    return zkStateWriter.writeStructureUpdates(collection);
  }

  public static class StateEntry {
    public ZkNodeProps message;
    public String znodeName;
  }

}
