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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.NodeHealthAPI;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

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
        rsp,
        new NodeHealthAPI(coreContainer).checkNodeHealth(requireHealthyCores, maxGenerationLag));
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
  public static long findUnhealthyCores(
      Collection<CloudDescriptor> cores, ClusterState clusterState) {
    return cores.stream()
        .filter(
            c ->
                !c.hasRegistered()
                    || UNHEALTHY_STATES.contains(c.getLastPublished())) // Find candidates locally
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
        .count();
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
    return Collections.emptyList();
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
