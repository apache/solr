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

import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.ListCollectionsApi;
import org.apache.solr.client.api.model.ListCollectionsResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for listing collections.
 *
 * <p>This API (GET /v2/collections) is equivalent to the v1 /admin/collections?action=LIST command
 */
public class ListCollections extends AdminAPIBase implements ListCollectionsApi {

  @Inject
  public ListCollections(CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req, rsp);
  }

  @Override
  @PermissionName(COLL_READ_PERM)
  public ListCollectionsResponse listCollections() {
    final ListCollectionsResponse response =
        instantiateJerseyResponse(ListCollectionsResponse.class);
    validateZooKeeperAwareCoreContainer(coreContainer);

    Map<String, DocCollection> collections =
        coreContainer.getZkController().getZkStateReader().getClusterState().getCollectionsMap();
    List<String> collectionList = new ArrayList<>(collections.keySet());
    Collections.sort(collectionList);
    // XXX should we add aliases here?
    response.collections = collectionList;

    return response;
  }
}
