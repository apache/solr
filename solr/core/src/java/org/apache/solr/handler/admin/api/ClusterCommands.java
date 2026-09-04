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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.client.solrj.response.RequestStatusState.COMPLETED;
import static org.apache.solr.client.solrj.response.RequestStatusState.FAILED;
import static org.apache.solr.client.solrj.response.RequestStatusState.NOT_FOUND;
import static org.apache.solr.client.solrj.response.RequestStatusState.RUNNING;
import static org.apache.solr.client.solrj.response.RequestStatusState.SUBMITTED;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Map;
import org.apache.solr.client.api.endpoint.ClusterCommandsApi;
import org.apache.solr.client.api.model.DeleteClusterCommandStatusResponse;
import org.apache.solr.client.api.model.GetClusterCommandStatusResponse;
import org.apache.solr.client.api.model.GetClusterCommandStatusResponse.CommandStatus;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerSolrResponseSerializer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for checking and deleting cluster-level asynchronous Collection API command status.
 *
 * <p>{@code GET /api/cluster/commands/{requestId}} is analogous to v1 {@code
 * /admin/collections?action=REQUESTSTATUS}. {@code DELETE /api/cluster/commands/{requestId}} and
 * {@code DELETE /api/cluster/commands} are analogous to v1 {@code
 * /admin/collections?action=DELETESTATUS}.
 *
 * <p>The v1 CollectionsHandler operations delegate to this class.
 */
public class ClusterCommands extends AdminAPIBase implements ClusterCommandsApi {

  @Inject
  public ClusterCommands(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_READ_PERM)
  public GetClusterCommandStatusResponse getClusterCommandStatus(String requestId)
      throws Exception {
    ensureRequiredParameterProvided(CoreAdminParams.REQUESTID, requestId);
    fetchAndValidateZooKeeperAwareCoreContainer();

    final GetClusterCommandStatusResponse response =
        instantiateJerseyResponse(GetClusterCommandStatusResponse.class);
    final ZkController zkController = coreContainer.getZkController();

    if (zkController.getDistributedCommandRunner().isEmpty()) {
      if (zkController.getOverseerRunningMap().contains(requestId)) {
        setStatus(response, RUNNING, requestId);
      } else if (zkController.getOverseerCompletedMap().contains(requestId)) {
        copyStoredCommandResponse(
            response,
            OverseerSolrResponseSerializer.deserialize(
                    zkController.getOverseerCompletedMap().get(requestId))
                .getResponse());
        setStatus(response, COMPLETED, requestId);
      } else if (zkController.getOverseerFailureMap().contains(requestId)) {
        copyStoredCommandResponse(
            response,
            OverseerSolrResponseSerializer.deserialize(
                    zkController.getOverseerFailureMap().get(requestId))
                .getResponse());
        setStatus(response, FAILED, requestId);
      } else if (overseerCollectionQueueContains(requestId)) {
        setStatus(response, SUBMITTED, requestId);
      } else {
        setStatus(response, NOT_FOUND, requestId);
      }
    } else {
      Pair<RequestStatusState, OverseerSolrResponse> sr =
          zkController.getDistributedCommandRunner().get().getAsyncTaskRequestStatus(requestId);
      switch (sr.first()) {
        case COMPLETED:
        case FAILED:
          copyStoredCommandResponse(response, sr.second().getResponse());
          break;
        default:
          break;
      }
      setStatus(response, sr.first(), requestId);
    }

    return response;
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public DeleteClusterCommandStatusResponse deleteClusterCommandStatus(String requestId)
      throws Exception {
    ensureRequiredParameterProvided(CoreAdminParams.REQUESTID, requestId);
    fetchAndValidateZooKeeperAwareCoreContainer();

    final DeleteClusterCommandStatusResponse response =
        instantiateJerseyResponse(DeleteClusterCommandStatusResponse.class);
    final ZkController zkController = coreContainer.getZkController();

    if (zkController.getDistributedCommandRunner().isEmpty()) {
      if (zkController.getOverseerCompletedMap().remove(requestId)) {
        zkController.clearAsyncId(requestId);
        response.status = "successfully removed stored response for [" + requestId + "]";
      } else if (zkController.getOverseerFailureMap().remove(requestId)) {
        zkController.clearAsyncId(requestId);
        response.status = "successfully removed stored response for [" + requestId + "]";
      } else {
        // Don't call zkController.clearAsyncId for this, since it could be a running/pending task
        response.status = "[" + requestId + "] not found in stored responses";
      }
    } else if (zkController.getDistributedCommandRunner().get().deleteSingleAsyncId(requestId)) {
      response.status = "successfully removed stored response for [" + requestId + "]";
    } else {
      response.status = "[" + requestId + "] not found in stored responses";
    }

    return response;
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public DeleteClusterCommandStatusResponse deleteAllClusterCommandStatuses() throws Exception {
    fetchAndValidateZooKeeperAwareCoreContainer();

    final DeleteClusterCommandStatusResponse response =
        instantiateJerseyResponse(DeleteClusterCommandStatusResponse.class);
    final ZkController zkController = coreContainer.getZkController();

    if (zkController.getDistributedCommandRunner().isEmpty()) {
      Collection<String> completed = zkController.getOverseerCompletedMap().keys();
      Collection<String> failed = zkController.getOverseerFailureMap().keys();
      for (String asyncId : completed) {
        zkController.getOverseerCompletedMap().remove(asyncId);
        zkController.clearAsyncId(asyncId);
      }
      for (String asyncId : failed) {
        zkController.getOverseerFailureMap().remove(asyncId);
        zkController.clearAsyncId(asyncId);
      }
    } else {
      zkController.getDistributedCommandRunner().get().deleteAllAsyncIds();
    }
    response.status = "successfully cleared stored collection api responses";
    return response;
  }

  /**
   * v1 {@code REQUESTSTATUS} entrypoint. Squashes the JAX-RS response into {@code rsp}, converting
   * the nested status object to a {@link SimpleOrderedMap} so SolrJ's v1 {@code RequestStatus}
   * parser continues to work.
   */
  public static void invokeGetFromV1Params(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req.getParams().required().check(REQUESTID);
    final ClusterCommands api = new ClusterCommands(coreContainer, req, rsp);
    final GetClusterCommandStatusResponse jerseyResponse =
        api.getClusterCommandStatus(req.getParams().get(REQUESTID));
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, jerseyResponse);
    convertStatusToNamedList(rsp);
  }

  /**
   * v1 {@code DELETESTATUS} entrypoint. {@code requestid} and {@code flush} remain mutually
   * exclusive query parameters on v1; the v2 API uses distinct paths instead.
   */
  public static void invokeDeleteFromV1Params(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final String requestId = req.getParams().get(REQUESTID);
    final boolean flush = req.getParams().getBool(CollectionAdminParams.FLUSH, false);

    if (requestId == null && !flush) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST, "Either requestid or flush parameter must be specified.");
    }
    if (requestId != null && flush) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Both requestid and flush parameters can not be specified together.");
    }

    final ClusterCommands api = new ClusterCommands(coreContainer, req, rsp);
    final DeleteClusterCommandStatusResponse jerseyResponse =
        flush ? api.deleteAllClusterCommandStatuses() : api.deleteClusterCommandStatus(requestId);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, jerseyResponse);
  }

  private boolean overseerCollectionQueueContains(String asyncId) throws Exception {
    return coreContainer
        .getZkController()
        .getOverseerCollectionQueue()
        .containsTaskWithRequestId(ASYNC, asyncId);
  }

  private static void setStatus(
      GetClusterCommandStatusResponse response, RequestStatusState state, String requestId) {
    final CommandStatus status = new CommandStatus();
    status.state = CommandStatus.State.valueOf(state.name());
    status.msg = statusMessage(state, requestId);
    response.status = status;
  }

  private static String statusMessage(RequestStatusState state, String requestId) {
    return switch (state) {
      case RUNNING -> "found [" + requestId + "] in running tasks";
      case COMPLETED -> "found [" + requestId + "] in completed tasks";
      case FAILED -> "found [" + requestId + "] in failed tasks";
      case SUBMITTED -> "found [" + requestId + "] in submitted tasks";
      default -> "Did not find [" + requestId + "] in any tasks queue";
    };
  }

  private static void copyStoredCommandResponse(
      GetClusterCommandStatusResponse response, NamedList<Object> stored) {
    if (stored == null) {
      return;
    }
    for (Map.Entry<String, Object> entry : stored) {
      final String key = entry.getKey();
      if ("responseHeader".equals(key) || "error".equals(key) || "status".equals(key)) {
        continue;
      }
      response.setUnknownProperty(key, entry.getValue());
    }
  }

  /**
   * v1 SolrJ reads {@code status} as a {@link NamedList}. Squash leaves the JAX-RS {@link
   * CommandStatus} POJO in place; replace it so existing clients keep working.
   */
  private static void convertStatusToNamedList(SolrQueryResponse rsp) {
    final NamedList<Object> values = rsp.getValues();
    final int idx = values.indexOf("status", 0);
    if (idx < 0) {
      return;
    }
    final Object statusVal = values.getVal(idx);
    if (statusVal instanceof CommandStatus commandStatus) {
      final SimpleOrderedMap<String> status = new SimpleOrderedMap<>();
      status.add("state", commandStatus.state.getKey());
      status.add("msg", commandStatus.msg);
      values.setVal(idx, status);
    }
  }
}
