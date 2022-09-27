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

import io.swagger.v3.oas.annotations.Parameter;
import org.apache.solr.client.solrj.request.beans.DeleteReplicaPropertyPayload;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import javax.inject.Inject;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * V2 API for removing a property from a collection replica
 *
 * <p>This API (POST /v2/collections/collectionName {'delete-replica-property': {...}}) is analogous
 * to the v1 /admin/collections?action=DELETEREPLICAPROP command.
 *
 * @see DeleteReplicaPropertyPayload
 */
@Path("/collections/{collName}/shards/{shardName}/replicas/{replicaName}/properties/{propName}")
public class DeleteReplicaPropertyAPI extends AdminAPIBase {

  @Inject
  public DeleteReplicaPropertyAPI(CoreContainer coreContainer,
                                  SolrQueryRequest solrQueryRequest,
                                  SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  public SolrJerseyResponse deleteReplicaProperty(
          @Parameter(
                  description = "The name of the collection the replica belongs to.",
                  required = true)
          @PathParam("collName")
          String collName,
          @Parameter(description = "The name of the shard the replica belongs to.", required = true)
          @PathParam("shardName")
          String shardName,
          @Parameter(description = "The replica, e.g., `core_node1`.", required = true)
          @PathParam("replicaName")
          String replicaName,
          @Parameter(description = "The name of the property to delete.", required = true)
          @PathParam("propName")
          String propertyName) {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    return response;
  }

}
