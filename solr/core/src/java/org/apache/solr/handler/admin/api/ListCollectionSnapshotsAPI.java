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
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

@Path("/collections/{collName}/snapshots")
public class ListCollectionSnapshotsAPI extends AdminAPIBase {

  @Inject
  public ListCollectionSnapshotsAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @GET
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_READ_PERM)
  public ListSnapshotsResponse listSnapshots(
      @Parameter(description = "The name of the collection.", required = true)
          @PathParam("collName")
          String collName)
      throws Exception {

    final ListSnapshotsResponse response = instantiateJerseyResponse(ListSnapshotsResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collName, solrQueryRequest);

    String collectionName =
        coreContainer
            .getZkController()
            .getZkStateReader()
            .getAliases()
            .resolveSimpleAlias(collName);
    ClusterState clusterState = coreContainer.getZkController().getClusterState();
    if (!clusterState.hasCollection(collectionName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Collection '" + collectionName + "' does not exist, no action taken.");
    }

    SolrZkClient client = coreContainer.getZkController().getZkClient();
    Collection<CollectionSnapshotMetaData> m =
        SolrSnapshotManager.listSnapshots(client, collectionName);

    Map<String, CollectionSnapshotMetaData> snapshots = new HashMap<>(m.size());
    for (CollectionSnapshotMetaData metaData : m) {
      snapshots.put(metaData.getName(), metaData);
    }

    response.snapshots = snapshots;

    return response;
  }

  public static class ListSnapshotsResponse extends SolrJerseyResponse {
    @Schema(description = "The snapshots for the collection.")
    @JsonProperty(SolrSnapshotManager.SNAPSHOTS_INFO)
    public Map<String, CollectionSnapshotMetaData> snapshots;
  }
}
