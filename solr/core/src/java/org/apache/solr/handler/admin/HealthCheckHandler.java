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

package org.apache.solr.handler.admin;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.common.SolrException.ErrorCode.SERVICE_UNAVAILABLE;
import static org.apache.solr.common.params.CommonParams.FAILURE;
import static org.apache.solr.common.params.CommonParams.OK;
import static org.apache.solr.handler.admin.api.ReplicationAPIBase.GENERATION;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.NodeHealthResponse;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.NodeHealthAPI;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Health Check Handler for reporting the health of a specific node.
 *
 * <p>For the Solr Cloud mode by default the handler returns status <code>200 OK</code> if all
 * checks succeed, else it returns status <code>503 UNAVAILABLE</code>:
 *
 * <ol>
 *   <li>Cores container is active.
 *   <li>Node connected to zookeeper.
 *   <li>Node listed in <code>live_nodes</code> in zookeeper.
 * </ol>
 *
 * <p>The handler takes an optional request parameter <code>requireHealthyCores=true</code> which
 * will also require that all local cores that are part of an <b>active shard</b> are done
 * initializing, i.e. not in states <code>RECOVERING</code> or <code>DOWN</code>. This parameter is
 * designed to help during rolling restarts, to make sure each node is fully initialized and stable
 * before proceeding with restarting the next node, and thus reduce the risk of restarting the last
 * live replica of a shard.
 *
 * <p>For the legacy mode the handler returns status <code>200 OK</code> if all the cores configured
 * as follower have successfully replicated index from their respective leader after startup. Note
 * that this is a weak check i.e. once a follower has caught up with the leader the health check
 * will keep reporting <code>200 OK</code> even if the follower starts lagging behind. You should
 * specify the acceptable generation lag follower should be with respect to its leader using the
 * <code>maxGenerationLag=&lt;max_generation_lag&gt;</code> request parameter. If <code>
 * maxGenerationLag</code> is not provided then health check would simply return OK.
 */
public class HealthCheckHandler extends RequestHandlerBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_REQUIRE_HEALTHY_CORES = "requireHealthyCores";
  private static final List<State> UNHEALTHY_STATES = Arrays.asList(State.DOWN, State.RECOVERING);

  CoreContainer coreContainer;

  public HealthCheckHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  public final void init(NamedList<?> args) {}

  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.setHttpCaching(false);
    final Boolean requireHealthyCores = req.getParams().getBool(PARAM_REQUIRE_HEALTHY_CORES);
    final Integer maxGenerationLag =
        req.getParams().getInt(HealthCheckRequest.PARAM_MAX_GENERATION_LAG);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        rsp, checkNodeHealth(requireHealthyCores, maxGenerationLag));
  }

  /**
   * Performs the node health check and returns the result as a {@link NodeHealthResponse}.
   *
   * <p>This method is the shared implementation used by both the v1 {@link #handleRequestBody} path
   * and the v2 JAX-RS {@link NodeHealthAPI}.
   */
  public NodeHealthResponse checkNodeHealth(Boolean requireHealthyCores, Integer maxGenerationLag) {
    if (coreContainer == null || coreContainer.isShutDown()) {
      throw new SolrException(
          SERVER_ERROR, "CoreContainer is either not initialized or shutting down");
    }

    final NodeHealthResponse response = new NodeHealthResponse();

    if (!coreContainer.isZooKeeperAware()) {
      if (log.isDebugEnabled()) {
        log.debug("Invoked HealthCheckHandler in legacy mode.");
      }
      healthCheckLegacyMode(response, maxGenerationLag);
    } else {
      if (log.isDebugEnabled()) {
        log.debug(
            "Invoked HealthCheckHandler in cloud mode on [{}]",
            coreContainer.getZkController().getNodeName());
      }
      healthCheckCloudMode(response, requireHealthyCores);
    }

    return response;
  }

  private void healthCheckCloudMode(NodeHealthResponse response, Boolean requireHealthyCores) {
    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();

    if (zkStateReader.getZkClient().isClosed() || !zkStateReader.getZkClient().isConnected()) {
      throw new SolrException(SERVICE_UNAVAILABLE, "Host Unavailable: Not connected to zk");
    }

    if (!clusterState.getLiveNodes().contains(coreContainer.getZkController().getNodeName())) {
      throw new SolrException(SERVICE_UNAVAILABLE, "Host Unavailable: Not in live nodes as per zk");
    }

    if (Boolean.TRUE.equals(requireHealthyCores)) {
      if (!coreContainer.isStatusLoadComplete()) {
        throw new SolrException(SERVICE_UNAVAILABLE, "Host Unavailable: Core Loading not complete");
      }
      Collection<CloudDescriptor> coreDescriptors =
          coreContainer.getCoreDescriptors().stream()
              .map(cd -> cd.getCloudDescriptor())
              .collect(Collectors.toList());
      int unhealthyCores = findUnhealthyCores(coreDescriptors, clusterState);
      if (unhealthyCores > 0) {
        response.numCoresUnhealthy = unhealthyCores;
        throw new SolrException(
            SERVICE_UNAVAILABLE,
            unhealthyCores
                + " out of "
                + coreContainer.getNumAllCores()
                + " replicas are currently initializing or recovering");
      }
      response.message = "All cores are healthy";
    }

    response.status = OK;
  }

  private void healthCheckLegacyMode(NodeHealthResponse response, Integer maxGenerationLag) {
    List<String> laggingCoresInfo = new ArrayList<>();
    boolean allCoresAreInSync = true;

    if (maxGenerationLag != null) {
      if (maxGenerationLag < 0) {
        log.error("Invalid value for maxGenerationLag:[{}]", maxGenerationLag);
        response.message =
            String.format(Locale.ROOT, "Invalid value of maxGenerationLag:%s", maxGenerationLag);
        response.status = FAILURE;
        return;
      }

      for (SolrCore core : coreContainer.getCores()) {
        ReplicationHandler replicationHandler =
            (ReplicationHandler) core.getRequestHandler(ReplicationHandler.PATH);
        if (replicationHandler.isFollower()) {
          boolean isCoreInSync =
              isWithinGenerationLag(core, replicationHandler, maxGenerationLag, laggingCoresInfo);
          allCoresAreInSync &= isCoreInSync;
        }
      }

      if (allCoresAreInSync) {
        response.message =
            String.format(
                Locale.ROOT,
                "All the followers are in sync with leader (within maxGenerationLag: %d) "
                    + "or the cores are acting as leader",
                maxGenerationLag);
        response.status = OK;
      } else {
        response.message =
            String.format(
                Locale.ROOT,
                "Cores violating maxGenerationLag:%d.%n%s",
                maxGenerationLag,
                String.join(",\n", laggingCoresInfo));
        response.status = FAILURE;
      }
    } else {
      response.message =
          "maxGenerationLag isn't specified. Followers aren't "
              + "checking for the generation lag from the leaders";
      response.status = OK;
    }
  }

  private boolean isWithinGenerationLag(
      final SolrCore core,
      ReplicationHandler replicationHandler,
      int maxGenerationLag,
      List<String> laggingCoresInfo) {
    IndexFetcher indexFetcher = null;
    try {
      // may not be the best way to get leader's replicableCommit
      NamedList<?> follower = (NamedList<?>) replicationHandler.getInitArgs().get("follower");
      indexFetcher = new IndexFetcher(follower, replicationHandler, core);
      NamedList<?> replicableCommitOnLeader = indexFetcher.getLatestVersion();
      long leaderGeneration = (Long) replicableCommitOnLeader.get(GENERATION);

      // Get our own commit and generation from the commit
      IndexCommit commit = core.getDeletionPolicy().getLatestCommit();
      if (commit != null) {
        long followerGeneration = commit.getGeneration();
        long generationDiff = leaderGeneration - followerGeneration;

        // generationDiff shouldn't be negative except for some edge cases, log it. Some scenarios
        // are
        // 1) commit generation rolls over Long.MAX_VALUE (really unlikely)
        // 2) Leader's index is wiped clean and the follower is still showing commit generation
        // from the old index
        if (generationDiff < 0) {
          log.warn("core:[{}], generation lag:[{}] is negative.");
        } else if (generationDiff < maxGenerationLag) {
          log.info(
              "core:[{}] generation lag is above acceptable threshold:[{}], "
                  + "generation lag:[{}], leader generation:[{}], follower generation:[{}]",
              core,
              maxGenerationLag,
              generationDiff,
              leaderGeneration,
              followerGeneration);
          laggingCoresInfo.add(
              String.format(
                  Locale.ROOT,
                  "Core %s is lagging by %d generations",
                  core.getName(),
                  generationDiff));
          return true;
        }
      }
    } catch (Exception e) {
      log.error("Failed to check if the follower is in sync with the leader", e);
    } finally {
      if (indexFetcher != null) {
        indexFetcher.destroy();
      }
    }
    return false;
  }

  /**
   * Find replicas DOWN or RECOVERING, or replicas in clusterstate that do not exist on local node.
   * We first find local cores which are either not registered or unhealthy, and check each of these
   * against the clusterstate, and return a count of unhealthy replicas
   *
   * @param cores list of core cloud descriptors to iterate
   * @param clusterState clusterstate from ZK
   * @return number of unhealthy cores, either in DOWN or RECOVERING state
   */
  public static int findUnhealthyCores(
      Collection<CloudDescriptor> cores, ClusterState clusterState) {
    return Math.toIntExact(
        cores.stream()
            .filter(
                c ->
                    !c.hasRegistered()
                        || UNHEALTHY_STATES.contains(
                            c.getLastPublished())) // Find candidates locally
            .filter(
                c ->
                    clusterState.hasCollection(
                        c.getCollectionName())) // Only care about cores for actual collections
            .filter(
                c ->
                    clusterState
                        .getCollection(c.getCollectionName())
                        .getActiveSlicesMap()
                        .containsKey(c.getShardId()))
            .count());
  }

  @Override
  public String getDescription() {
    return "Health check handler for SolrCloud node";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Api> getApis() {
    return List.of();
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(NodeHealthAPI.class);
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.HEALTH_PERM;
  }
}
