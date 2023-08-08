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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.api.collections.InstallShardDataCmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A V2 API that allows users to import an index constructed offline into a shard of their
 * collection
 *
 * <p>Particularly useful for installing (per-shard) indices constructed offline into a SolrCloud
 * deployment. Callers are required to put the collection into read-only mode prior to installing
 * data into any shards of that collection, and should exit read only mode when completed.
 */
@Path("/collections/{collName}/shards/{shardName}/install")
public class InstallShardDataAPI extends AdminAPIBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  public InstallShardDataAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse installShardData(
      @PathParam("collName") String collName,
      @PathParam("shardName") String shardName,
      InstallShardRequestBody requestBody)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collName, solrQueryRequest);
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required request body missing");
    }

    if (StringUtils.isBlank(requestBody.location)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The Install Shard Data API requires a 'location' indicating the index data to install");
    }

    final ClusterState clusterState =
        coreContainer.getZkController().getZkStateReader().getClusterState();
    ensureCollectionAndShardExist(clusterState, collName, shardName);

    // Only install data to shards which belong to a collection in read-only mode
    final DocCollection dc =
        coreContainer.getZkController().getZkStateReader().getCollection(collName);
    if (!dc.isReadOnly()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Collection must be in readOnly mode before installing data to shard");
    }

    final ZkNodeProps remoteMessage = createRemoteMessage(collName, shardName, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.INSTALLSHARDDATA,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    return response;
  }

  public static void ensureCollectionAndShardExist(
      ClusterState clusterState, String collectionName, String shardName) {
    final DocCollection installCollection = clusterState.getCollection(collectionName);
    final Slice installSlice = installCollection.getSlice(shardName);
    if (installSlice == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The specified shard [" + shardName + "] does not exist.");
    }
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, String shardName, InstallShardRequestBody requestBody) {
    final InstallShardDataCmd.RemoteMessage messageTyped = new InstallShardDataCmd.RemoteMessage();
    messageTyped.collection = collectionName;
    messageTyped.shard = shardName;
    if (requestBody != null) {
      messageTyped.location = requestBody.location;
      messageTyped.repository = requestBody.repository;
      messageTyped.asyncId = requestBody.asyncId;
    }

    messageTyped.validate();
    return new ZkNodeProps(messageTyped.toMap(new HashMap<>()));
  }

  public static class InstallShardRequestBody implements JacksonReflectMapWriter {
    @JsonProperty(defaultValue = "location", required = true)
    public String location;

    @JsonProperty("repository")
    public String repository;

    @JsonProperty("async")
    public String asyncId;
  }
}
