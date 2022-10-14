/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin.api;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.CreateShardPayload;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;

/**
 * V2 API for creating a new shard in a collection.
 *
 * <p>This API (POST /v2/collections/collectionName/shards {'create': {...}}) is analogous to the v1
 * /admin/collections?action=CREATESHARD command.
 *
 * @see CreateShardAPI
 */
@EndPoint(
    path = {"/c/{collection}/shards", "/collections/{collection}/shards"},
    method = POST,
    permission = COLL_EDIT_PERM)
public class CreateShardAPI {
  private static final String V2_CREATE_CMD = "create";

  private final CollectionsHandler collectionsHandler;

  public CreateShardAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @Command(name = V2_CREATE_CMD)
  public void createShard(PayloadObj<CreateShardPayload> obj) throws Exception {
    final CreateShardPayload v2Body = obj.get();
    final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
    v1Params.put(ACTION, CollectionParams.CollectionAction.CREATESHARD.toLower());
    v1Params.put(COLLECTION, obj.getRequest().getPathTemplateValues().get(COLLECTION));

    if (v2Body.nodeSet != null) {
      v1Params.put(CREATE_NODE_SET_PARAM, buildV1CreateNodeSetValue(v2Body.nodeSet));
    }

    if (MapUtils.isNotEmpty(v2Body.coreProperties)) {
      flattenMapWithPrefix(v2Body.coreProperties, v1Params, PROPERTY_PREFIX);
    }
    collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }

  private String buildV1CreateNodeSetValue(List<String> nodeSet) {
    if (nodeSet.size() > 0) {
      return String.join(",", nodeSet);
    }
    return "EMPTY";
  }
}
