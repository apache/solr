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

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ID;

import com.codahale.metrics.Timer;
import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.solr.cloud.Overseer.LeaderStatus;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic processor run in the Overseer, used for handling items added to a distributed work
 * queue. Has support for handling exclusive tasks (i.e. tasks that should not run in parallel with
 * each other).
 *
 * <p>An {@link OverseerMessageHandlerSelector} determines which {@link OverseerMessageHandler}
 * handles specific messages in the queue.
 */
public class OverseerTaskProcessor implements Runnable, Closeable {

  /** Maximum number of overseer collection operations which can be executed concurrently */
  public static final int MAX_PARALLEL_TASKS = 100;

  public static final int MAX_BLOCKED_TASKS = 1000;

  public ExecutorService tpe;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerTaskQueue workQueue;
  private final DistributedMap runningMap;
  private final DistributedMap completedMap;
  private final DistributedMap failureMap;

  /**
   * All the tasks that have been submitted to the runner thread pool. This is keyed on zk id of the
   * task. It may contain tasks that have completed execution, have been entered into the
   * completed/failed map in zk but not deleted from the work-queue as that is a batched operation.
   * This collection is accessed only in the main loop and does not need to be synchronized.
   */
  private final Map<String, Future<?>> runningTasks;

  private final String myId;

  private final ZkStateReader zkStateReader;

  private boolean isClosed;

  private final Stats stats;
  private final SolrMetricsContext overseerTaskProcessorMetricsContext;

  /**
   * This map may contain tasks which are read from work queue but could not be executed because
   * they are blocked or the execution queue is full This is an optimization to ensure that we do
   * not read the same tasks again and again from ZK.
   */
  private final Map<String, QueueEvent> blockedTasks =
      Collections.synchronizedMap(new LinkedHashMap<>());

  /**
   * Predicate used to filter out tasks from the Zookeeper queue that should not be returned for
   * processing.
   */
  private final Predicate<String> excludedTasks =
      new Predicate<>() {
        @Override
        public boolean test(String s) {
          return runningTasks.containsKey(s) || blockedTasks.containsKey(s);
        }

        @Override
        public String toString() {
          return StrUtils.join(Set.of(runningTasks, blockedTasks.keySet()), ',');
        }
      };

  private final Object waitLock = new Object();

  protected OverseerMessageHandlerSelector selector;

  private final OverseerNodePrioritizer prioritizer;

  private final String thisNode;

  public OverseerTaskProcessor(
      ZkStateReader zkStateReader,
      String myId,
      Stats stats,
      OverseerMessageHandlerSelector selector,
      OverseerNodePrioritizer prioritizer,
      OverseerTaskQueue workQueue,
      DistributedMap runningMap,
      DistributedMap completedMap,
      DistributedMap failureMap,
      SolrMetricsContext solrMetricsContext) {
    this.zkStateReader = zkStateReader;
    this.myId = myId;
    this.stats = stats;
    this.selector = selector;
    this.prioritizer = prioritizer;
    this.workQueue = workQueue;
    this.runningMap = runningMap;
    this.completedMap = completedMap;
    this.failureMap = failureMap;
    this.runningTasks = new HashMap<>();
    thisNode = MDCLoggingContext.getNodeName();

    overseerTaskProcessorMetricsContext = solrMetricsContext.getChildContext(this);
    overseerTaskProcessorMetricsContext.gauge(
        () -> workQueue.getZkStats().getQueueLength(), true, "collectionWorkQueueSize", "queue");
  }

  @Override
  public void run() {
    MDCLoggingContext.setNode(thisNode);
    log.debug("Process current queue of overseer operations");
    LeaderStatus isLeader = amILeader();
    while (isLeader == LeaderStatus.DONT_KNOW) {
      log.debug("am_i_leader unclear {}", isLeader);
      isLeader = amILeader(); // not a no, not a yes, try ask again
    }

    String oldestItemInWorkQueue = null;
    // hasLeftOverItems - used for avoiding re-execution of async tasks that were processed by a
    // previous Overseer. This variable is set in case there's any task found on the workQueue when
    // the OCP starts up and the id for the queue tail is used as a marker to check for the task in
    // completed/failed map in zk. Beyond the marker, all tasks can safely be assumed to have never
    // been executed.
    boolean hasLeftOverItems = true;

    try {
      oldestItemInWorkQueue = workQueue.getTailId();
    } catch (KeeperException e) {
      // We don't need to handle this. This is just a fail-safe which comes in handy in skipping
      // already processed async calls.
      log.error("KeeperException", e);
    } catch (IllegalStateException ignore) {
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (oldestItemInWorkQueue == null) hasLeftOverItems = false;
    else
      log.debug(
          "Found already existing elements in the work-queue. Last element: {}",
          oldestItemInWorkQueue);

    try {
      prioritizer.prioritizeOverseerNodes(myId);
    } catch (IllegalStateException ignore) {
      return;
    } catch (Exception e) {
      if (!zkStateReader.getZkClient().isClosed()) {
        log.error("Unable to prioritize overseer ", e);
      }
    }

    // TODO: Make maxThreads configurable.

    this.tpe =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            5,
            MAX_PARALLEL_TASKS,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            new SolrNamedThreadFactory("OverseerThreadFactory"));

    // In OverseerCollectionMessageHandler, a new Session needs to be created for each new iteration
    // over the tasks in the queue. Incrementing this id causes a new session to be created there.
    long batchSessionId = 0;

    try {
      while (!this.isClosed) {
        try {
          isLeader = amILeader();
          if (LeaderStatus.NO == isLeader) {
            break;
          } else if (LeaderStatus.YES != isLeader) {
            log.debug("am_i_leader unclear {}", isLeader);
            continue; // not a no, not a yes, try asking again
          }

          cleanUpRunningTasks();

          printTrackingMaps();

          boolean waited = false;

          while (runningTasks.size() > MAX_PARALLEL_TASKS) {
            synchronized (waitLock) {
              waitLock.wait(100); // wait for 100 ms or till a task is complete
            }
            waited = true;
          }

          if (waited) {
            cleanUpRunningTasks();
          }

          ArrayList<QueueEvent> heads = new ArrayList<>(blockedTasks.size() + MAX_PARALLEL_TASKS);
          heads.addAll(blockedTasks.values());

          // If we have enough items in the blocked tasks already, it makes
          // no sense to read more items from the work queue. it makes sense
          // to clear out at least a few items in the queue before we read more items
          if (heads.size() < MAX_BLOCKED_TASKS) {
            // instead of reading MAX_PARALLEL_TASKS items always, we should only fetch as much as
            // we can execute
            int toFetch =
                Math.min(
                    MAX_BLOCKED_TASKS - heads.size(), MAX_PARALLEL_TASKS - runningTasks.size());
            List<QueueEvent> newTasks = workQueue.peekTopN(toFetch, excludedTasks, 2000L);
            if (log.isDebugEnabled()) {
              log.debug("Got {} tasks from work-queue : [{}]", newTasks.size(), newTasks);
            }
            // heads has at most MAX_BLOCKED_TASKS tasks.
            heads.addAll(newTasks);
          } else {
            // The sleep below slows down spinning when heads is full from previous work dispatch
            // attempt below and no new tasks got executed (all executors are busy or all waiting
            // tasks require locks currently held by executors).
            //
            // When heads is not full but no progress was made (no new work got dispatched in the
            // for loop below), slowing down of the spinning is done by the wait time in the call to
            // workQueue.peekTopN() above. (at least in theory because the method eventually called
            // from there is ZkDistributedQueue.peekElements() and because it filters out entries
            // that have just completed on a Runner thread in a different way than the predicate
            // based filtering, it can return quickly without waiting the configured delay time.
            // Therefore spinning can be observed, likely something to clean up at some point).
            //
            // If heads is not empty and new tasks appeared in the queue there's no delay,
            // workQueue.peekTopN() above will return immediately.
            Thread.sleep(1000);
          }

          if (isClosed) break;

          if (heads.isEmpty()) {
            continue;
          }

          // clear the blocked tasks, may get refilled below. Given blockedTasks can only get
          // entries from heads and heads has at most MAX_BLOCKED_TASKS tasks, blockedTasks will
          // never exceed MAX_BLOCKED_TASKS entries.
          // Note blockedTasks can't be cleared too early as it is used in the excludedTasks
          // Predicate above.
          blockedTasks.clear();

          // Trigger the creation of a new Session used for locking when/if a lock is later acquired
          // on the OverseerCollectionMessageHandler
          batchSessionId++;

          for (int i = 0; i < heads.size(); i++) {
            QueueEvent head = heads.get(i);

            if (runningTasks.size() >= MAX_PARALLEL_TASKS) {
              // Too many tasks are running, just shove the rest into the "blocked" queue.
              heads.subList(i, heads.size()).forEach(h -> blockedTasks.put(h.getId(), h));
              break;
            }

            if (runningTasks.containsKey(head.getId())) continue;
            final ZkNodeProps message = ZkNodeProps.load(head.getBytes());
            final String asyncId = message.getStr(ASYNC);
            if (hasLeftOverItems) {
              if (head.getId().equals(oldestItemInWorkQueue)) hasLeftOverItems = false;
              if (asyncId != null
                  && (completedMap.contains(asyncId) || failureMap.contains(asyncId))) {
                log.debug(
                    "Found already processed task in workQueue, cleaning up. AsyncId [{}]",
                    asyncId);
                workQueue.remove(head, false);
                continue;
              }
            }
            String operation = message.getStr(Overseer.QUEUE_OPERATION);
            if (operation == null) {
              log.error("Msg does not have required {} : {}", Overseer.QUEUE_OPERATION, message);
              workQueue.remove(head, asyncId == null);
              continue;
            }
            OverseerMessageHandler messageHandler = selector.selectOverseerMessageHandler(message);
            OverseerMessageHandler.Lock lock = messageHandler.lockTask(message, batchSessionId);
            if (lock == null) {
              if (log.isDebugEnabled()) {
                log.debug("Exclusivity check failed for [{}]", message);
              }
              blockedTasks.put(head.getId(), head);
              continue;
            }
            try {
              if (asyncId != null) {
                // Store the async ID in ZK distributed map before trying to execute the task
                runningMap.put(asyncId, null);
              }

              if (log.isDebugEnabled()) {
                log.debug("Marked task [{}] as running", head.getId());
              }
            } catch (KeeperException.NodeExistsException e) {
              lock.unlock();
              // This should never happen
              log.error("Tried to pick up task [{}] when it was already running!", head.getId());
              continue;
            } catch (InterruptedException e) {
              lock.unlock();
              log.error(
                  "Thread interrupted while trying to pick task {} for execution.", head.getId());
              Thread.currentThread().interrupt();
              continue;
            }
            if (log.isDebugEnabled()) {
              log.debug(
                  "{}: Get the message id: {} message: {}",
                  messageHandler.getName(),
                  head.getId(),
                  message);
            }
            Runner runner = createRunner(messageHandler, message, operation, head, lock);
            Future<?> future = tpe.submit(runner);
            runningTasks.put(head.getId(), future);
          }

        } catch (KeeperException e) {
          if (e.code() == KeeperException.Code.SESSIONEXPIRED) {
            log.warn("Overseer cannot talk to ZK");
            return;
          }
          log.error("KeeperException", e);

          // Prevent free-spinning this loop.
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            return;
          }

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (IllegalStateException ignore) {

        } catch (Exception e) {
          log.error("Exception processing", e);
        }
      }
    } finally {
      this.close();
    }
  }

  /** Remove all entries from {@link #runningTasks} that are done. */
  private void cleanUpRunningTasks() {
    if (log.isDebugEnabled()) {
      log.debug("Cleaning up running tasks. #Running tasks: {}", runningTasks.size());
    }
    runningTasks.entrySet().removeIf(e -> e.getValue().isDone());
  }

  @Override
  public void close() {
    isClosed = true;
    overseerTaskProcessorMetricsContext.unregister();
    if (tpe != null) {
      if (!ExecutorUtil.isShutdown(tpe)) {
        ExecutorUtil.shutdownAndAwaitTermination(tpe);
      }
    }
    IOUtils.closeQuietly(selector);
  }

  public static List<String> getSortedOverseerNodeNames(SolrZkClient zk)
      throws KeeperException, InterruptedException {
    List<String> children =
        getSortedElectionNodes(zk, Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE);

    ArrayList<String> nodeNames = new ArrayList<>(children.size());
    for (String c : children) nodeNames.add(LeaderElector.getNodeName(c));
    return nodeNames;
  }

  public static List<String> getSortedElectionNodes(SolrZkClient zk, String path)
      throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren(path, null, true);
    LeaderElector.sortSeqs(children);
    return children;
  }

  public static String getLeaderNode(SolrZkClient zkClient)
      throws KeeperException, InterruptedException {
    String id = getLeaderId(zkClient);
    return id == null ? null : LeaderElector.getNodeName(id);
  }

  public static String getLeaderId(SolrZkClient zkClient)
      throws KeeperException, InterruptedException {
    byte[] data = null;
    try {
      data = zkClient.getData(Overseer.OVERSEER_ELECT + "/leader", null, new Stat(), true);
    } catch (KeeperException.NoNodeException e) {
      return null;
    }
    Map<?, ?> m = (Map<?, ?>) Utils.fromJSON(data);
    return (String) m.get(ID);
  }

  protected LeaderStatus amILeader() {
    String statsName = "collection_am_i_leader";
    Timer.Context timerContext = stats.time(statsName);
    boolean success = true;
    String propsId = null;
    try {
      ZkNodeProps props =
          ZkNodeProps.load(
              zkStateReader
                  .getZkClient()
                  .getData(Overseer.OVERSEER_ELECT + "/leader", null, null, true));
      propsId = props.getStr(ID);
      if (myId.equals(propsId)) {
        return LeaderStatus.YES;
      }
    } catch (KeeperException e) {
      success = false;
      if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
        log.error("", e);
        return LeaderStatus.DONT_KNOW;
      } else if (e.code() != KeeperException.Code.SESSIONEXPIRED) {
        log.warn("", e);
      } else {
        log.debug("", e);
      }
    } catch (IllegalStateException e) {
      success = false;
    } catch (InterruptedException e) {
      success = false;
      Thread.currentThread().interrupt();
    } finally {
      timerContext.stop();
      if (success) {
        stats.success(statsName);
      } else {
        stats.error(statsName);
      }
    }
    log.info("According to ZK I (id={}) am no longer a leader. propsId={}", myId, propsId);
    return LeaderStatus.NO;
  }

  public boolean isClosed() {
    return isClosed;
  }

  /**
   * Create a runner instance to execute a single task. This method mosty exists to provide an
   * extension point for tests.
   */
  protected Runner createRunner(
      OverseerMessageHandler messageHandler,
      ZkNodeProps message,
      String operation,
      QueueEvent head,
      OverseerMessageHandler.Lock lock) {
    return new Runner(messageHandler, message, operation, head, lock);
  }

  protected class Runner implements Runnable {
    ZkNodeProps message;
    String operation;
    OverseerSolrResponse response;
    QueueEvent head;
    OverseerMessageHandler messageHandler;
    private final OverseerMessageHandler.Lock lock;

    public Runner(
        OverseerMessageHandler messageHandler,
        ZkNodeProps message,
        String operation,
        QueueEvent head,
        OverseerMessageHandler.Lock lock) {
      this.message = message;
      this.operation = operation;
      this.head = head;
      this.messageHandler = messageHandler;
      this.lock = lock;
      response = null;
    }

    @Override
    public void run() {
      String statsName = messageHandler.getTimerName(operation);
      final Timer.Context timerContext = stats.time(statsName);

      boolean success = false;
      final String asyncId = message.getStr(ASYNC);
      String taskKey = messageHandler.getTaskKey(message);

      try {
        try {
          if (log.isDebugEnabled()) {
            log.debug("Runner processing {}", head.getId());
          }
          response = messageHandler.processMessage(message, operation);
        } finally {
          timerContext.stop();
          updateStats(statsName);
        }

        if (asyncId != null) {
          if (response != null
              && (response.getResponse().get("failure") != null
                  || response.getResponse().get("exception") != null)) {
            failureMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response));
            if (log.isDebugEnabled()) {
              log.debug("Updated failed map for task with zkid:[{}]", head.getId());
            }
          } else {
            completedMap.put(asyncId, OverseerSolrResponseSerializer.serialize(response));
            if (log.isDebugEnabled()) {
              log.debug("Updated completed map for task with zkid:[{}]", head.getId());
            }
          }
        } else {
          head.setBytes(OverseerSolrResponseSerializer.serialize(response));
          if (log.isDebugEnabled()) {
            log.debug("Completed task:[{}]", head.getId());
          }
        }

        markTaskComplete(asyncId);
        if (log.isDebugEnabled()) {
          log.debug("Marked task [{}] as completed.", head.getId());
        }
        printTrackingMaps();

        if (log.isDebugEnabled()) {
          log.debug(
              "{}: Message id: {} complete, response: {}",
              messageHandler.getName(),
              head.getId(),
              response.getResponse());
        }
        success = true;
      } catch (IllegalStateException ignore) {

      } catch (KeeperException e) {
        log.error("KeeperException", e);
      } catch (InterruptedException e) {
        // Reset task from tracking data structures so that it can be retried.
        resetTaskWithException(head.getId(), asyncId, taskKey);
        log.warn("Resetting task {} as the thread was interrupted.", head.getId());
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
        if (!success) {
          // Reset task from tracking data structures so that it can be retried.
          try {
            resetTaskWithException(head.getId(), asyncId, taskKey);
          } catch (IllegalStateException ignore) {

          }
        }
        synchronized (waitLock) {
          waitLock.notifyAll();
        }
      }
    }

    /**
     * Invoked by the runner thread once the task is successfully completed (can be with error
     * status). We remove the task from the work queue.
     */
    private void markTaskComplete(String asyncId) throws KeeperException, InterruptedException {

      if (asyncId != null) {
        if (!runningMap.remove(asyncId)) {
          log.warn("Could not find and remove async call [{}] from the running map.", asyncId);
        }
      }

      workQueue.remove(head, asyncId == null);
    }

    /**
     * Reset the task so it will be retried by a future iteration of the main loop. We remove the
     * async ID from the running map, but we keep the task in the work queue.
     */
    private void resetTaskWithException(String id, String asyncId, String taskKey) {
      log.warn("Resetting task: {}, requestid: {}, taskKey: {}", id, asyncId, taskKey);
      try {
        if (asyncId != null) {
          if (!runningMap.remove(asyncId)) {
            log.warn("Could not find and remove async call [{}] from the running map.", asyncId);
          }
        }

      } catch (KeeperException e) {
        log.error("KeeperException", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private void updateStats(String statsName) {
      if (isSuccessful()) {
        stats.success(statsName);
      } else {
        stats.error(statsName);
        stats.storeFailureDetails(statsName, message, response);
      }
    }

    private boolean isSuccessful() {
      if (response == null) return false;
      return !(response.getResponse().get("failure") != null
          || response.getResponse().get("exception") != null);
    }
  }

  private void printTrackingMaps() {
    if (log.isDebugEnabled()) {
      log.debug("RunningTasks: {}", runningTasks.keySet());
      log.debug("BlockedTasks: {}", blockedTasks.keySet()); // nowarn
    }
  }

  String getId() {
    return myId;
  }

  /**
   * An interface to determine which {@link OverseerMessageHandler} handles a given message. This
   * could be a single OverseerMessageHandler for the case where a single type of message is handled
   * (e.g. collection messages only) , or a different handler could be selected based on the
   * contents of the message.
   */
  public interface OverseerMessageHandlerSelector extends Closeable {
    OverseerMessageHandler selectOverseerMessageHandler(ZkNodeProps message);
  }
}
