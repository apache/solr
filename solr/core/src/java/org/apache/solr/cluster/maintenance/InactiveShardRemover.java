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

package org.apache.solr.cluster.maintenance;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.api.ConfigurablePlugin;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This Cluster Singleton can be configured to periodically find and remove inactive Shards */
public class InactiveShardRemover
    implements ClusterSingleton, ConfigurablePlugin<InactiveShardRemoverConfig> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PLUGIN_NAME = ".inactive-shard-remover";

  static class DeleteActor {

    private final CoreContainer coreContainer;

    DeleteActor(final CoreContainer coreContainer) {
      this.coreContainer = coreContainer;
    }

    void delete(final Slice slice, final String asyncId) throws IOException {
      CollectionAdminRequest.DeleteShard deleteRequest =
          CollectionAdminRequest.deleteShard(slice.getCollection(), slice.getName());
      deleteRequest.setAsyncId(asyncId);
      coreContainer.getZkController().getSolrCloudManager().request(deleteRequest);
    }
  }

  private State state = State.STOPPED;

  private final CoreContainer coreContainer;

  private final DeleteActor deleteActor;

  private ScheduledExecutorService executor;

  private long scheduleIntervalSeconds;

  private long ttlSeconds;

  private int maxDeletesPerCycle;

  /** Constructor invoked via Reflection */
  public InactiveShardRemover(final CoreContainer cc) {
    this(cc, new DeleteActor(cc));
  }

  @VisibleForTesting
  InactiveShardRemover(final CoreContainer cc, final DeleteActor actor) {
    this.coreContainer = cc;
    this.deleteActor = actor;
  }

  @Override
  public void configure(final InactiveShardRemoverConfig cfg) {
    this.scheduleIntervalSeconds = cfg.scheduleIntervalSeconds;
    this.maxDeletesPerCycle = cfg.maxDeletesPerCycle;
    this.ttlSeconds = cfg.ttlSeconds;
  }

  @Override
  public String getName() {
    return PLUGIN_NAME;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void start() throws Exception {
    state = State.STARTING;
    executor = Executors.newScheduledThreadPool(1, new SolrNamedThreadFactory(PLUGIN_NAME));
    executor.scheduleAtFixedRate(
        this::deleteInactiveSlices,
        scheduleIntervalSeconds,
        scheduleIntervalSeconds,
        TimeUnit.SECONDS);
    state = State.RUNNING;
  }

  @Override
  public void stop() {
    if (state == State.RUNNING) {
      state = State.STOPPING;
      executor.shutdownNow();
      try {
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          log.warn(
              "Executor pool did not terminate within the specified timeout: {} {}",
              10,
              TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        log.warn("Failed to shut down the executor pool", e);
        Thread.currentThread().interrupt();
      }
    }
    state = State.STOPPED;
  }

  @VisibleForTesting
  void deleteInactiveSlices() {
    final ClusterState clusterState = coreContainer.getZkController().getClusterState();
    Collection<Slice> inactiveSlices = new HashSet<>();
    clusterState
        .getCollectionsMap()
        .forEach((k, v) -> inactiveSlices.addAll(collectInactiveSlices(v)));

    if (log.isInfoEnabled()) {
      log.info(
          "Found {} inactive Shards to delete, {} will be deleted",
          inactiveSlices.size(),
          Math.max(inactiveSlices.size(), maxDeletesPerCycle));
    }

    inactiveSlices.stream().limit(maxDeletesPerCycle).forEach(this::deleteShard);
  }

  private Collection<Slice> collectInactiveSlices(final DocCollection docCollection) {
    final Collection<Slice> slices = new HashSet<>(docCollection.getSlices());
    slices.removeAll(docCollection.getActiveSlices());
    return slices.stream().filter(this::isExpired).collect(Collectors.toList());
  }

  private void deleteShard(final Slice s) {
    final String asyncId = s.getCollection() + ";" + s.getName() + ";DELETESHARD";
    try {
      final RequestStatusState status = getStatus(asyncId);
      if (status != RequestStatusState.FAILED && status != RequestStatusState.NOT_FOUND) {
        log.info(
            "Delete Shard request is already submitted or in progress with asyncId {}", asyncId);
        return;
      }

      if (status == RequestStatusState.FAILED) {
        // Delete, so that we can try again
        deleteStatus(asyncId);
      }

      deleteActor.delete(s, asyncId);
    } catch (IOException e) {
      log.warn("An exception occurred when deleting an inactive shard with asyncId {}", asyncId);
    }
  }

  /**
   * An Inactive Shard is expired if it has not undergone a state change in the period of time
   * defined by {@link InactiveShardRemover#ttlSeconds}. If it is expired, it is eligible for
   * removal.
   */
  private boolean isExpired(final Slice slice) {

    final String collectionName = slice.getCollection();
    final String sliceName = slice.getName();

    if (slice.getState() != Slice.State.INACTIVE) {
      return false;
    }

    final String lastChangeTimestamp = slice.getStr(ZkStateReader.STATE_TIMESTAMP_PROP);
    if (lastChangeTimestamp == null || lastChangeTimestamp.isEmpty()) {
      log.warn(
          "Collection {} Shard {} has no last change timestamp and will not be deleted",
          collectionName,
          sliceName);
      return false;
    }

    final long epochTimestamp;
    try {
      epochTimestamp = Long.parseLong(lastChangeTimestamp);
    } catch (NumberFormatException e) {
      log.warn(
          "Collection {} Shard {} has an invalid last change timestamp and will not be deleted",
          collectionName,
          sliceName);
      return false;
    }

    long currentEpochTime =
        coreContainer.getZkController().getSolrCloudManager().getTimeSource().getEpochTimeNs();
    long delta = TimeUnit.NANOSECONDS.toSeconds(currentEpochTime - epochTimestamp);

    boolean expired = delta >= ttlSeconds;
    if (log.isDebugEnabled()) {
      log.debug(
          "collection {} shard {} last state change {} seconds ago. Expired={}",
          slice.getCollection(),
          slice.getName(),
          delta,
          expired);
    }
    return expired;
  }

  private RequestStatusState getStatus(final String asyncId) throws IOException {
    CollectionAdminRequest.RequestStatus req = CollectionAdminRequest.requestStatus(asyncId);
    CollectionAdminRequest.RequestStatusResponse statusResponse =
        coreContainer.getZkController().getSolrCloudManager().request(req);
    return statusResponse.getRequestStatus();
  }

  private void deleteStatus(final String asyncId) throws IOException {
    CollectionAdminRequest.DeleteStatus req = CollectionAdminRequest.deleteAsyncId(asyncId);
    CollectionAdminResponse rsp =
        coreContainer.getZkController().getSolrCloudManager().request(req);
    if (log.isInfoEnabled()) {
      log.info(String.valueOf(rsp.getResponse().get("status")));
    }
  }
}
