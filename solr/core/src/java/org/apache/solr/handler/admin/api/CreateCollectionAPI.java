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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.ROUTER_KEY;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.CreatePayload;
import org.apache.solr.client.solrj.request.beans.V2ApiConstants;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;

@EndPoint(
    path = {"/collections"},
    method = POST,
    permission = COLL_EDIT_PERM)
public class CreateCollectionAPI {

  public static final String V2_CREATE_COLLECTION_CMD = "create";

  private final CollectionsHandler collectionsHandler;

  public CreateCollectionAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @Command(name = V2_CREATE_COLLECTION_CMD)
  public void create(PayloadObj<CreatePayload> obj) throws Exception {
    final CreatePayload v2Body = obj.get();
    final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

    v1Params.put(ACTION, CollectionParams.CollectionAction.CREATE.toLower());
    convertV2CreateCollectionMapToV1ParamMap(v1Params);

    collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }

  @SuppressWarnings("unchecked")
  public static void convertV2CreateCollectionMapToV1ParamMap(Map<String, Object> v2MapVals) {
    // Keys are copied so that map can be modified as keys are looped through.
    final Set<String> v2Keys = v2MapVals.keySet().stream().collect(Collectors.toSet());
    for (String key : v2Keys) {
      switch (key) {
        case V2ApiConstants.PROPERTIES_KEY:
          final Map<String, Object> propertiesMap =
              (Map<String, Object>) v2MapVals.remove(V2ApiConstants.PROPERTIES_KEY);
          flattenMapWithPrefix(propertiesMap, v2MapVals, PROPERTY_PREFIX);
          break;
        case ROUTER_KEY:
          final Map<String, Object> routerProperties =
              (Map<String, Object>) v2MapVals.remove(V2ApiConstants.ROUTER_KEY);
          flattenMapWithPrefix(routerProperties, v2MapVals, CollectionAdminParams.ROUTER_PREFIX);
          break;
        case V2ApiConstants.CONFIG:
          v2MapVals.put(CollectionAdminParams.COLL_CONF, v2MapVals.remove(V2ApiConstants.CONFIG));
          break;
        case V2ApiConstants.SHUFFLE_NODES:
          v2MapVals.put(
              CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM,
              v2MapVals.remove(V2ApiConstants.SHUFFLE_NODES));
          break;
        case V2ApiConstants.NODE_SET:
          final Object nodeSetValUncast = v2MapVals.remove(V2ApiConstants.NODE_SET);
          if (nodeSetValUncast instanceof String) {
            v2MapVals.put(CollectionAdminParams.CREATE_NODE_SET_PARAM, nodeSetValUncast);
          } else {
            final List<String> nodeSetList = (List<String>) nodeSetValUncast;
            final String nodeSetStr = String.join(",", nodeSetList);
            v2MapVals.put(CollectionAdminParams.CREATE_NODE_SET_PARAM, nodeSetStr);
          }
          break;
        default:
          break;
      }
    }
  }
}
