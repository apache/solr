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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.handler.admin.ClusterStatus.INCLUDE_ALL;

import jakarta.inject.Inject;
import org.apache.solr.client.api.endpoint.CollectionStatusApi;
import org.apache.solr.client.api.model.CollectionStatusResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ClusterStatus;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

/**
 * V2 API for displaying basic information about a single collection.
 *
 * <p>This API (GET /v2/collections/collectionName) is analogous to the v1
 * /admin/collections?action=CLUSTERSTATUS&amp;collection=collectionName command.
 */
public class CollectionStatus extends AdminAPIBase implements CollectionStatusApi {

  @Inject
  public CollectionStatus(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @PermissionName(PermissionNameProvider.Name.COLL_READ_PERM)
  @Override
  public CollectionStatusResponse getCollectionStatus(String collectionName) throws Exception {
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    // Fetch the NamedList from ClusterStatus...
    final var nlResponse = new NamedList<>();
    final var params = new ModifiableSolrParams();
    params.set(INCLUDE_ALL, false);
    params.set(COLLECTION_PROP, collectionName);
    // TODO Rework ClusterStatus to avoid the intermediate NL, if all usages can be switched over
    new ClusterStatus(coreContainer.getZkController().getZkStateReader(), params)
        .getClusterStatus(nlResponse);

    // ...and convert it to the response type
    final var collMetadata = nlResponse.findRecursive("cluster", "collections", collectionName);
    return SolrJacksonMapper.getObjectMapper()
        .convertValue(collMetadata, CollectionStatusResponse.class);
  }
}
