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

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class holding the implementation required for tracking asynchronous Collection API (or other)
 * tasks when the Collection API is distributed.
 *
 * <p>This replaces the features provided by the distributed maps on ZK paths
 * /overseer/collection-map-completed, /overseer/collection-map-failure and /overseer/async_ids when
 * the Collection API commands are handled by the Overseer.
 *
 * <p>It works by using two Zookeeper directories, one for persistent nodes for each new async id
 * and one for ephemeral nodes for each async id currently being processed (in flight).<br>
 * A persistent async node has either no data, or has a serialized OverseerSolrResponse as content.
 * An ephemeral async node has two possible states (content): 'S' or 'R'.
 *
 * <p>The actual state of an async task is built from a combination of the two nodes:
 *
 * <pre>
 * +===================+=========================================+=================================================+====================+
 * |                   | persistent=success OverseerSolrResponse | persistent=null or failed OverseerSolrResponse  | No persistent node |
 * +===================+=========================================+=================================================+====================+
 * | ephemeral="S"     | Task completed successfully             | Task submitted                                  | Unknown task       |
 * +-------------------+-----------------------------------------+-------------------------------------------------+--------------------+
 * | ephemeral="R"     | Task completed successfully             | Task running                                    | Unknown task       |
 * +-------------------+-----------------------------------------+-------------------------------------------------+--------------------+
 * | No ephemeral node | Task completed successfully             | Task failed (see response or null=node failure) | Unknown task       |
 * +-------------------+-----------------------------------------+-------------------------------------------------+--------------------+
 * </pre>
 */
public class DistributedApiAsyncTracker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Corresponds to Overseer.NUM_RESPONSES_TO_STORE. The size of the persistent store of async ID's
   * put in Zookeeper. This is the max total tracked async request ID's over all nodes running in
   * the distributed Collection API.
   */
  public static int MAX_TRACKED_ASYNC_TASKS = 10000;

  private static final String ZK_ASYNC_PERSISTENT = "/persistent";
  private static final String ZK_ASYNC_INFLIGHT = "/inflight";

  private final String persistentIdsPath;
  private final String inFlightIdsPath;

  /**
   * Persistent storage in Zookeeper under path {@link #ZK_ASYNC_PERSISTENT} of all currently known
   * (in flight, completed with success or error) async request id's.
   */
  private final SizeLimitedDistributedMap trackedAsyncTasks;

  private final InFlightJobs inFlightAsyncTasks;

  public DistributedApiAsyncTracker(SolrZkClient zkClient, String rootPath) {
    this(zkClient, rootPath, MAX_TRACKED_ASYNC_TASKS);
  }

  @VisibleForTesting
  DistributedApiAsyncTracker(SolrZkClient zkClient, String rootPath, int maxTrackedTasks) {
    persistentIdsPath = rootPath + ZK_ASYNC_PERSISTENT;
    inFlightIdsPath = rootPath + ZK_ASYNC_INFLIGHT;

    trackedAsyncTasks =
        new SizeLimitedDistributedMap(zkClient, persistentIdsPath, maxTrackedTasks, null);
    inFlightAsyncTasks = new InFlightJobs(zkClient, inFlightIdsPath);
  }

  /**
   * After a successful call to this method, caller MUST eventually call {@link #setTaskCompleted}
   * or {@link #cancelAsyncId} otherwise the task will forever be considered as in progress.
   *
   * @param asyncId if {@code null} this method will do nothing.
   * @return {@code true} if the asyncId was not already in use (or is {@code null}) and {@code
   *     false} if it is already in use and can't be allocated again.
   */
  public boolean createNewAsyncJobTracker(String asyncId) {
    if (asyncId == null) {
      return true;
    }
    try {
      // First create the persistent node, with no content. If that fails, it means the asyncId has
      // been previously used and not yet cleared...
      if (!trackedAsyncTasks.putIfAbsent(asyncId, null)) {
        return false;
      }

      // ...then create the transient node. If the corresponding ephemeral node already exists, it
      // means the persistent node was removed (maybe trackedAsyncTasks grew too large? It has a max
      // size then evicts). We cannot then track the new provided asyncId, and have simply "revived"
      // its persistent node...
      try {
        inFlightAsyncTasks.createNewInFlightTask(asyncId);
        return true;
      } catch (KeeperException.NodeExistsException nee) {
        log.warn(
            "Async id {} was not found in trackedAsyncTasks but was still present in inFlightAsyncTasks",
            asyncId);
        return false;
      }
    } catch (KeeperException ke) {
      throw new SolrException(SERVER_ERROR, "Error creating new async job tracking " + asyncId, ke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SERVER_ERROR, "Interrupted creating new async job tracking " + asyncId, ie);
    }
  }

  /**
   * Initially an async task is submitted. Just before it actually starts execution it is set to
   * running.
   */
  public void setTaskRunning(String asyncId) {
    if (asyncId == null) {
      return;
    }
    try {
      inFlightAsyncTasks.setTaskRunning(asyncId);
    } catch (KeeperException ke) {
      throw new SolrException(SERVER_ERROR, "Error setting async task as running " + asyncId, ke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SERVER_ERROR, "Interrupted setting async task as running " + asyncId, ie);
    }
  }

  /**
   * Mark the completion (success or error) of an async task. The success or error is judged by the
   * contents of the {@link OverseerSolrResponse}.
   */
  public void setTaskCompleted(String asyncId, OverseerSolrResponse solrResponse) {
    if (asyncId == null) {
      return;
    }
    // First update the persistent node with the execution result, only then remove the transient
    // node (otherwise a status check might report the task in error)
    try {
      try {
        trackedAsyncTasks.put(asyncId, OverseerSolrResponseSerializer.serialize(solrResponse));
      } finally {
        inFlightAsyncTasks.deleteInFlightTask(asyncId);
      }
    } catch (KeeperException ke) {
      throw new SolrException(SERVER_ERROR, "Error setting async task as completed " + asyncId, ke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SERVER_ERROR, "Interrupted setting async task as completed " + asyncId, ie);
    }
  }

  /** Cancels the tracking of an asyncId, if the corresponding command could not be executed. */
  public void cancelAsyncId(String asyncId) {
    if (asyncId == null) {
      return;
    }
    try {
      try {
        trackedAsyncTasks.remove(asyncId);
      } finally {
        inFlightAsyncTasks.deleteInFlightTask(asyncId);
      }
    } catch (KeeperException ke) {
      throw new SolrException(SERVER_ERROR, "Error canceling async task " + asyncId, ke);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new SolrException(SERVER_ERROR, "Interrupted canceling async task " + asyncId, ie);
    }
  }

  /**
   * This method implements the logic described in the class Javadoc table ({@link
   * DistributedApiAsyncTracker}), using the two sets of tracking info to build the actual state of
   * an async task.
   *
   * <p>Returns the status of an async task, and when relevant the corresponding response from the
   * command execution. The returned {@link OverseerSolrResponse} will not be {@code null} when the
   * returned {@link RequestStatusState} is {@link RequestStatusState#COMPLETED} or {@link
   * RequestStatusState#FAILED} (and will be {@code null} in all other cases).
   */
  public Pair<RequestStatusState, OverseerSolrResponse> getAsyncTaskRequestStatus(String asyncId)
      throws Exception {
    if (asyncId == null || !trackedAsyncTasks.contains(asyncId)) {
      // This return addresses the whole "No persistent node" column from the table
      return new Pair<>(RequestStatusState.NOT_FOUND, null);
    }

    byte[] data = trackedAsyncTasks.get(asyncId);
    OverseerSolrResponse response =
        data != null ? OverseerSolrResponseSerializer.deserialize(data) : null;

    if (response != null
        && response.getResponse().get("failure") == null
        && response.getResponse().get("exception") == null) {
      // This return addresses the whole "persistent=success OverseerSolrResponse" column from the
      // table
      return new Pair<>(RequestStatusState.COMPLETED, response);
    }

    // Now dealing with the middle column "persistent=null or failed OverseerSolrResponse"
    InFlightJobs.State ephemeralState = inFlightAsyncTasks.getInFlightState(asyncId);
    if (ephemeralState == InFlightJobs.State.SUBMITTED) {
      return new Pair<>(RequestStatusState.SUBMITTED, null);
    } else if (ephemeralState == InFlightJobs.State.RUNNING) {
      return new Pair<>(RequestStatusState.RUNNING, null);
    }

    // The task has failed, but there are two options: if response is null, it has failed because
    // the node on which it was running has crashed. If it is not null, it has failed because the
    // execution has failed. Because caller expects a non null response in any case, let's make up
    // one if needed...
    if (response == null) {
      // Node crash has removed the ephemeral node, but the command did not complete execution (or
      // didn't even start it, who knows). We have a failure to report though so let's create a
      // reasonable return response.
      NamedList<Object> results = new NamedList<>();
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      nl.add(
          "msg",
          "Operation (asyncId: " + asyncId + ") failed due to server restart. Please resubmit.");
      nl.add("rspCode", SERVER_ERROR.code);
      results.add("exception", nl);
      response = new OverseerSolrResponse(results);
    }

    return new Pair<>(RequestStatusState.FAILED, response);
  }

  /**
   * Deletes a single async tracking ID if the corresponding job has completed or failed.
   *
   * @return {@code true} if the {@code asyncId} was found to be of a completed or failed job and
   *     was successfully removed, {@code false} if the id was not found or was found for a
   *     submitted or running job (these are not removed).
   */
  public boolean deleteSingleAsyncId(String asyncId) throws Exception {
    return inFlightAsyncTasks.getInFlightState(asyncId) == InFlightJobs.State.NOT_FOUND
        && trackedAsyncTasks.remove(asyncId);
  }

  /**
   * Deletes all async id's for completed or failed async jobs. Does not touch id's for submitted or
   * running jobs.
   */
  public void deleteAllAsyncIds() throws Exception {
    Collection<String> allTracked = trackedAsyncTasks.keys();

    for (String asyncId : allTracked) {
      deleteSingleAsyncId(asyncId);
    }
  }

  /**
   * Manages the ephemeral nodes for tasks currently being processed (running or waiting for a lock)
   * by a node.
   */
  private static class InFlightJobs {
    enum State {
      SUBMITTED("S"),
      RUNNING("R"),
      NOT_FOUND(null);

      private final String shorthand;

      State(String shorthand) {
        this.shorthand = shorthand;
      }
    }

    private final SolrZkClient zkClient;
    private final String rootNodePath;

    InFlightJobs(SolrZkClient zkClient, String rootNodePath) {
      this.zkClient = zkClient;
      this.rootNodePath = rootNodePath;

      try {
        if (!zkClient.exists(rootNodePath, true)) {
          zkClient.makePath(rootNodePath, new byte[0], CreateMode.PERSISTENT, true);
        }
      } catch (KeeperException.NodeExistsException nee) {
        // Some other thread (on this or another JVM) beat us to create the node, that's ok, the
        // node exists.
      } catch (KeeperException ke) {
        throw new SolrException(SERVER_ERROR, "Error creating root node " + rootNodePath, ke);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new SolrException(SERVER_ERROR, "Interrupted creating root node " + rootNodePath, ie);
      }
    }

    void createNewInFlightTask(String asyncId) throws KeeperException, InterruptedException {
      zkClient.create(
          getPath(asyncId),
          State.SUBMITTED.shorthand.getBytes(StandardCharsets.UTF_8),
          CreateMode.EPHEMERAL,
          true);
    }

    void setTaskRunning(String asyncId) throws KeeperException, InterruptedException {
      zkClient.setData(
          getPath(asyncId), State.RUNNING.shorthand.getBytes(StandardCharsets.UTF_8), true);
    }

    void deleteInFlightTask(String asyncId) throws KeeperException, InterruptedException {
      zkClient.delete(getPath(asyncId), -1, true);
    }

    State getInFlightState(String asyncId) throws KeeperException, InterruptedException {
      if (!zkClient.exists(getPath(asyncId), true)) {
        return State.NOT_FOUND;
      }

      final byte[] bytes;
      try {
        bytes = zkClient.getData(getPath(asyncId), null, null, true);
      } catch (KeeperException.NoNodeException nne) {
        // Unlikely race, but not impossible...
        if (log.isInfoEnabled()) {
          log.info(
              "AsyncId ephemeral node "
                  + getPath(asyncId)
                  + " vanished from underneath us. Funny."); // nowarn
        }
        return State.NOT_FOUND;
      }

      if (bytes == null) {
        // This is not expected. The ephemeral nodes are always created with content.
        log.error(
            "AsyncId ephemeral node "
                + getPath(asyncId)
                + " has null content. This is unexpected (bug)."); // nowarn
        return State.NOT_FOUND;
      }

      String content = new String(bytes, StandardCharsets.UTF_8);
      if (State.RUNNING.shorthand.equals(content)) {
        return State.RUNNING;
      } else if (State.SUBMITTED.shorthand.equals(content)) {
        return State.SUBMITTED;
      } else {
        log.error(
            "AsyncId ephemeral node "
                + getPath(asyncId)
                + " has unexpected content \""
                + content
                + "\". This is unexpected (bug)."); // nowarn
        return State.NOT_FOUND;
      }
    }

    private String getPath(String asyncId) {
      return rootNodePath + DistributedCollectionConfigSetCommandRunner.ZK_PATH_SEPARATOR + asyncId;
    }
  }
}
