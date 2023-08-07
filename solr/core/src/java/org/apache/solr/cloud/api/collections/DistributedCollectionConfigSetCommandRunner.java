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

package org.apache.solr.cloud.api.collections;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.ConfigSetApiLockFactory;
import org.apache.solr.cloud.ConfigSetCmds;
import org.apache.solr.cloud.DistributedApiAsyncTracker;
import org.apache.solr.cloud.DistributedMultiLock;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.ZkDistributedCollectionLockFactory;
import org.apache.solr.cloud.ZkDistributedConfigSetLockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for execution Collection API and Config Set API commands in a distributed way, without
 * going through Overseer and {@link OverseerCollectionMessageHandler} or {@link
 * org.apache.solr.cloud.OverseerConfigSetMessageHandler}.
 *
 * <p>This class is only called when Collection and Config Set API calls are configured to be
 * distributed, which implies cluster state updates are distributed as well.
 */
public class DistributedCollectionConfigSetCommandRunner {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String ZK_PATH_SEPARATOR = "/";

  private static final String ZK_DISTRIBUTED_API_ROOT = "/distributedapi";

  /** Zookeeper node below which the locking hierarchy is anchored */
  private static final String ZK_COLLECTION_LOCKS = ZK_DISTRIBUTED_API_ROOT + "/collectionlocks";

  private static final String ZK_CONFIG_SET_LOCKS = ZK_DISTRIBUTED_API_ROOT + "/configsetlocks";

  /** Zookeeper node below which the async id (requestId) tracking is done (for async requests) */
  private static final String ZK_ASYNC_ROOT = ZK_DISTRIBUTED_API_ROOT + "/async";

  private final ExecutorService distributedCollectionApiExecutorService;
  /**
   * All Collection API commands are executed as if they are asynchronous to stick to the same
   * behavior as the Overseer based Collection API execution. The difference between sync and async
   * is twofold: 1. Non async execution does wait for a while to see if the command has completed
   * and if so, returns success (and if not, returns failure) 2. There is no way to query the status
   * of a non async command that is still running, or to kill it (short of shutting down the node on
   * which it's running). This is not the best design, but at this stage that's what happens with
   * Overseer based Collection API, so implementing the same.
   *
   * <p>The common aspects between sync and async is that actual command execution is given an
   * infinite time to eventually complete (or fail). Again this is not great, but that's how it is
   * for now. Likely some cleanup required later (once Overseer is actually remove? Like Solr 10 or
   * 11?).
   */
  private final ExecutorService commandsExecutor;

  private final CoreContainer coreContainer;
  private final CollApiCmds.CommandMap commandMapper;
  private final CollectionCommandContext ccc;
  private final DistributedApiAsyncTracker asyncTaskTracker;

  private volatile boolean shuttingDown = false;

  public DistributedCollectionConfigSetCommandRunner(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;

    if (log.isInfoEnabled()) {
      // Note is it hard to print a log when Collection API is handled by Overseer because Overseer
      // is started regardless of how Collection API is handled, so it doesn't really know...
      log.info(
          "Creating DistributedCollectionConfigSetCommandRunner. Collection and ConfigSet APIs are running distributed (not Overseer based)");
    }

    // TODO we should look at how everything is getting closed when the node is shutdown. But it
    // seems that CollectionsHandler (that creates instances of this class) is not really closed, so
    // maybe it doesn't matter?
    // With distributed Collection API execution, each node will have such an executor but given how
    // thread pools work, threads will only be created if needed (including the corePoolSize
    // threads).
    distributedCollectionApiExecutorService =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            5,
            10,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            new SolrNamedThreadFactory("DistributedCollectionApiExecutorService"));

    commandsExecutor =
        new ExecutorUtil.MDCAwareThreadPoolExecutor(
            5,
            20,
            0L,
            TimeUnit.MILLISECONDS,
            new SynchronousQueue<>(),
            new SolrNamedThreadFactory("DistributedCollectionApiCommandExecutor"));

    ccc =
        new DistributedCollectionCommandContext(
            this.coreContainer, this.distributedCollectionApiExecutorService);
    commandMapper = new CollApiCmds.CommandMap(ccc);
    asyncTaskTracker =
        new DistributedApiAsyncTracker(ccc.getZkStateReader().getZkClient(), ZK_ASYNC_ROOT);
  }

  /** See {@link DistributedApiAsyncTracker#getAsyncTaskRequestStatus(String)} */
  public Pair<RequestStatusState, OverseerSolrResponse> getAsyncTaskRequestStatus(String asyncId)
      throws Exception {
    return asyncTaskTracker.getAsyncTaskRequestStatus(asyncId);
  }

  /** See {@link DistributedApiAsyncTracker#deleteSingleAsyncId(String)} */
  public boolean deleteSingleAsyncId(String asyncId) throws Exception {
    return asyncTaskTracker.deleteSingleAsyncId(asyncId);
  }

  /** See {@link DistributedApiAsyncTracker#deleteAllAsyncIds()} */
  public void deleteAllAsyncIds() throws Exception {
    asyncTaskTracker.deleteAllAsyncIds();
  }

  /**
   * When {@link org.apache.solr.handler.admin.CollectionsHandler#invokeAction} does not enqueue to
   * overseer queue and instead calls this method, this method is expected to do the equivalent of
   * what Overseer does in {@link
   * org.apache.solr.cloud.OverseerConfigSetMessageHandler#processMessage}.
   *
   * <p>The steps leading to that call in the Overseer execution path are (and the equivalent is
   * done here):
   *
   * <ul>
   *   <li>{@link org.apache.solr.cloud.OverseerTaskProcessor#run()} gets the message from the ZK
   *       queue, grabs the corresponding locks (write lock on the config set target of the API
   *       command and a read lock on the base config set if any - the case for config set creation)
   *       then executes the command using an executor service (it also checks the asyncId if any is
   *       specified but async calls are not supported for Config Set API calls).
   *   <li>In {@link org.apache.solr.cloud.OverseerTaskProcessor}.{@code Runner.run()} (run on an
   *       executor thread) a call is made to {@link
   *       org.apache.solr.cloud.OverseerConfigSetMessageHandler#processMessage} which does a few
   *       checks and calls the appropriate Config Set method.
   * </ul>
   */
  public void runConfigSetCommand(
      SolrQueryResponse rsp,
      ConfigSetParams.ConfigSetAction action,
      Map<String, Object> result,
      long timeoutMs)
      throws Exception {
    // We refuse new tasks, but will wait for already submitted ones (i.e. those that made it
    // through this method earlier). See stopAndWaitForPendingTasksToComplete() below
    if (shuttingDown) {
      throw new SolrException(
          SolrException.ErrorCode.CONFLICT,
          "Solr is shutting down, no more Config Set API tasks may be executed");
    }

    // never null
    String configSetName = (String) result.get(NAME);
    // baseConfigSetName will be null if we're not creating a new config set
    String baseConfigSetName =
        ConfigSetCmds.getBaseConfigSetName(
            action, (String) result.get(ConfigSetCmds.BASE_CONFIGSET));

    if (log.isInfoEnabled()) {
      log.info("Running Config Set API locally for " + action + " " + configSetName); // nowarn
    }

    ConfigSetCommandRunner commandRunner =
        new ConfigSetCommandRunner(
            new ZkNodeProps(result), action, configSetName, baseConfigSetName);
    final Future<Void> taskFuture;
    try {
      taskFuture = commandsExecutor.submit(commandRunner);
    } catch (RejectedExecutionException ree) {
      throw new SolrException(
          SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Too many executing commands", ree);
    }

    // Wait for a while... Just like Overseer based Config Set API (wait can timeout but actual
    // command execution does not)
    try {
      taskFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          action + " " + configSetName + " timed out after " + timeoutMs + "ms");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, action + " " + configSetName + " interrupted", e);
    }
  }

  /**
   * When {@link org.apache.solr.handler.admin.CollectionsHandler#invokeAction} does not enqueue to
   * overseer queue and instead calls this method, this method is expected to do the equivalent of
   * what Overseer does in {@link
   * org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler#processMessage}.
   *
   * <p>The steps leading to that call in the Overseer execution path are (and the equivalent is
   * done here):
   *
   * <ul>
   *   <li>{@link org.apache.solr.cloud.OverseerTaskProcessor#run()} gets the message from the ZK
   *       queue, grabs the corresponding lock (Collection API calls do locking to prevent non
   *       compatible concurrent modifications of a collection), marks the async id of the task as
   *       running then executes the command using an executor service
   *   <li>In {@link org.apache.solr.cloud.OverseerTaskProcessor}.{@code Runner.run()} (run on an
   *       executor thread) a call is made to {@link
   *       org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler#processMessage}
   *       which sets the logging context, calls {@link CollApiCmds.CollectionApiCommand#call}
   * </ul>
   */
  public OverseerSolrResponse runCollectionCommand(
      ZkNodeProps message, CollectionParams.CollectionAction action, long timeoutMs) {
    // We refuse new tasks, but will wait for already submitted ones (i.e. those that made it
    // through this method earlier). See stopAndWaitForPendingTasksToComplete() below
    if (shuttingDown) {
      throw new SolrException(
          SolrException.ErrorCode.CONFLICT,
          "Solr is shutting down, no more Collection API tasks may be executed");
    }

    final String asyncId = message.getStr(ASYNC);

    if (log.isInfoEnabled()) {
      log.info(
          "Running Collection API locally for " + action.name() + " asyncId=" + asyncId); // nowarn
    }

    // Following the call below returning true, we must eventually cancel or complete the task.
    // Happens either in the CollectionCommandRunner below or in the catch when the runner would not
    // execute.
    if (!asyncTaskTracker.createNewAsyncJobTracker(asyncId)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Task with the same requestid already exists. (" + asyncId + ")");
    }

    CollectionCommandRunner commandRunner = new CollectionCommandRunner(message, action, asyncId);
    final Future<OverseerSolrResponse> taskFuture;
    try {
      taskFuture = commandsExecutor.submit(commandRunner);
    } catch (RejectedExecutionException ree) {
      // The command will not run, need to cancel the async ID so it can be reused on a subsequent
      // attempt by the client
      asyncTaskTracker.cancelAsyncId(asyncId);
      throw new SolrException(
          SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Too many executing commands", ree);
    }

    if (asyncId == null) {
      // Non async calls wait for a while in case the command completes. If they time out, there's
      // no way to track the job progress (improvement suggestion: decorrelate having a task ID from
      // the fact of waiting for the job to complete)
      try {
        return taskFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (TimeoutException te) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, action + " timed out after " + timeoutMs + "ms");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, action + " interrupted", e);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, action + " failed", e);
      }
    } else {
      // Async calls do not wait for the command to finish but get instead back the async id (that
      // they just sent...)
      NamedList<Object> resp = new NamedList<>();
      resp.add(CoreAdminParams.REQUESTID, asyncId);
      return new OverseerSolrResponse(resp);
    }
  }

  /**
   * Best effort wait for termination of all tasks for a (short) while, then return. This method is
   * called when the JVM shuts down so tasks that did not complete are expected to be stopped mid
   * processing.
   */
  public void stopAndWaitForPendingTasksToComplete() {
    shuttingDown = true;

    commandsExecutor.shutdown();
    distributedCollectionApiExecutorService.shutdown();

    // Duration we are willing to wait for threads to terminate, total.
    final long TOTAL_WAIT_NS = 10L * 1000 * 1000 * 1000;

    long start = System.nanoTime();
    try {
      commandsExecutor.awaitTermination(TOTAL_WAIT_NS, TimeUnit.NANOSECONDS);
      long remaining = TOTAL_WAIT_NS - (System.nanoTime() - start);
      if (remaining > 0L) {
        distributedCollectionApiExecutorService.awaitTermination(remaining, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Collection name can be found in either of two message parameters (why??). Return it from where
   * it's defined. (see also parameter {@code collectionNameParamName} of {@link
   * org.apache.solr.cloud.DistributedClusterStateUpdater.MutatingCommand#MutatingCommand(CollectionParams.CollectionAction,
   * String)})
   */
  public static String getCollectionName(ZkNodeProps message) {
    return message.containsKey(COLLECTION_PROP)
        ? message.getStr(COLLECTION_PROP)
        : message.getStr(NAME);
  }

  /**
   * All Collection API commands are executed in separate threads so that the command can run to
   * completion even if the client request times out, or for async tasks, the client request returns
   * and the command is executed (which really is very similar). This provides a behavior very
   * similar to the one provided by Overseer based Collection API execution.
   */
  private class CollectionCommandRunner implements Callable<OverseerSolrResponse> {
    private final ZkNodeProps message;
    private final CollectionParams.CollectionAction action;
    private final String asyncId;

    private CollectionCommandRunner(
        ZkNodeProps message, CollectionParams.CollectionAction action, String asyncId) {
      this.message = message;
      this.action = action;
      this.asyncId = asyncId;
    }

    /**
     * This method does always free the Collection API lock it grabs and updates the tracking of the
     * {@code asyncId} (initiated before this method got called) on all execution paths out of this
     * method!
     */
    @Override
    public OverseerSolrResponse call() {
      final String collName = getCollectionName(message);
      final String shardId = message.getStr(SHARD_ID_PROP);
      final String replicaName = message.getStr(REPLICA_PROP);

      MDCLoggingContext.setCollection(collName);
      MDCLoggingContext.setShard(shardId);
      MDCLoggingContext.setReplica(replicaName);

      NamedList<Object> results = new NamedList<>();
      try {
        // Create API lock for executing the command. This call is non blocking (not blocked on
        // waiting for a lock to be acquired anyway, might be blocked on access to ZK etc) We create
        // a new CollectionApiLockFactory using a new ZkDistributedCollectionLockFactory because
        // earlier (in the constructor of this class) the ZkStateReader was not yet available, due
        // to how CoreContainer is built in part in the constructor and in part in its load()
        // method. And this class is built from the CoreContainer constructor... The cost of these
        // creations is low, and these classes do not hold state but only serve as an interface to
        // Zookeeper. Note that after this call, we MUST execute the lock.release(); in the finally
        // below
        DistributedMultiLock lock =
            new CollectionApiLockFactory(
                    new ZkDistributedCollectionLockFactory(
                        ccc.getZkStateReader().getZkClient(), ZK_COLLECTION_LOCKS))
                .createCollectionApiLock(action.lockLevel, collName, shardId, replicaName);

        try {
          log.debug(
              "CollectionCommandRunner about to acquire lock for action {} lock level {}. {}/{}/{}",
              action,
              action.lockLevel,
              collName,
              shardId,
              replicaName);

          // Block this thread until all required locks are acquired.
          lock.waitUntilAcquired();

          // Got the lock so moving from submitted to running if we run for an async task (if
          // asyncId is null the asyncTaskTracker calls do nothing).
          asyncTaskTracker.setTaskRunning(asyncId);

          log.debug(
              "DistributedCollectionConfigSetCommandRunner.runCollectionCommand. Lock acquired. Calling: {}, {}",
              action,
              message);

          CollApiCmds.CollectionApiCommand command = commandMapper.getActionCommand(action);
          if (command != null) {
            command.call(ccc.getSolrCloudManager().getClusterState(), message, results);
          } else {
            asyncTaskTracker.cancelAsyncId(asyncId);
            // Seeing this is a bug, not bad user data
            String message = "Bug: Unknown operation " + action;
            log.error(message);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, message);
          }
        } finally {
          try {
            // TODO If the Collection API command failed because the collection does not exist,
            // we've just created some lock directory structure for a non existent collection...
            // Maybe try to remove it here? No big deal for now as leftover nodes in the lock
            // hierarchy do no harm, and there shouldn't be too many of those.
            lock.release();
          } catch (SolrException se) {
            log.error(
                "Error when releasing collection locks for operation " + action, se); // nowarn
          }
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        // Output some error logs
        if (collName == null) {
          log.error("Operation {} failed", action, e);
        } else {
          log.error("Collection {}}, operation {} failed", collName, action, e);
        }

        results.add("Operation " + action + " caused exception:", e);
        SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
        nl.add("msg", e.getMessage());
        nl.add("rspCode", e instanceof SolrException ? ((SolrException) e).code() : -1);
        results.add("exception", nl);
      }

      OverseerSolrResponse res = new OverseerSolrResponse(results);
      // Following call marks success or failure depending on the contents of res
      asyncTaskTracker.setTaskCompleted(asyncId, res);
      return res;
    }
  }

  /**
   * All Config Set API commands are executed in separate threads so that the command can run to
   * completion even if the client request times out.<br>
   * This provides a behavior very similar to the one provided by Overseer based Config Set API
   * execution.
   *
   * <p>The logic of this class is similar to (and simpler than) the one of {@link
   * CollectionCommandRunner}, more detailed comments there.
   */
  private class ConfigSetCommandRunner implements Callable<Void> {
    private final ZkNodeProps message;
    private final ConfigSetParams.ConfigSetAction action;
    private final String configSetName;
    private final String baseConfigSetName;

    private ConfigSetCommandRunner(
        ZkNodeProps message,
        ConfigSetParams.ConfigSetAction action,
        String configSetName,
        String baseConfigSetName) {
      this.message = message;
      this.action = action;
      this.configSetName = configSetName;
      this.baseConfigSetName = baseConfigSetName;
    }

    /** This method does always free the Collection API lock it grabs. */
    @Override
    public Void call() throws IOException {
      // After this call, we MUST execute the lock.release(); in the finally below
      DistributedMultiLock lock =
          new ConfigSetApiLockFactory(
                  new ZkDistributedConfigSetLockFactory(
                      ccc.getZkStateReader().getZkClient(), ZK_CONFIG_SET_LOCKS))
              .createConfigSetApiLock(configSetName, baseConfigSetName);

      try {
        log.debug(
            "ConfigSetCommandRunner about to acquire lock for action {} config set {} base config set {}",
            action,
            configSetName,
            baseConfigSetName);

        // Block this thread until all required locks are acquired.
        lock.waitUntilAcquired();

        log.debug("ConfigSetCommandRunner. Lock acquired. Calling: {}, {}", action, message);

        switch (action) {
          case CREATE:
            ConfigSetCmds.createConfigSet(message, coreContainer);
            break;
          case DELETE:
            ConfigSetCmds.deleteConfigSet(message, coreContainer);
            break;
          default:
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "Bug! Unknown Config Set action: " + action);
        }
      } finally {
        lock.release();
      }

      return null;
    }
  }
}
