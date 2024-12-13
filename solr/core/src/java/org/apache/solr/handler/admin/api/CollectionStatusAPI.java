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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import java.lang.invoke.MethodHandles;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API for displaying basic information about a single collection.
 *
 * <p>This API (GET /v2/collections/collectionName) is analogous to the v1
 * /admin/collections?action=CLUSTERSTATUS&amp;collection=collectionName command.
 */
public class CollectionStatusAPI {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionsHandler collectionsHandler;

  public CollectionStatusAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @EndPoint(
      path = {"/c/{collection}", "/collections/{collection}"},
      method = GET,
      permission = COLL_READ_PERM)
  public void getCollectionStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req =
        wrapParams(
            req, // 'req' can have a 'shard' param
            ACTION,
            CollectionParams.CollectionAction.CLUSTERSTATUS.toString(),
            COLLECTION,
            req.getPathTemplateValues().get(ZkStateReader.COLLECTION_PROP));
    collectionsHandler.handleRequestBody(req, rsp);
  }
}
