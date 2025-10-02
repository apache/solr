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

import static org.apache.solr.client.api.model.Constants.ASYNC;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.MODIFIABLE_COLLECTION_PROPERTIES;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.PER_REPLICA_STATE;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.READ_ONLY;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.handler.admin.api.CreateCollection.copyPrefixedPropertiesWithoutPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.client.api.endpoint.CollectionApis;
import org.apache.solr.client.api.model.ModifyCollectionRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementation for modifying collections.
 *
 * <p>The new API (PUT /v2/collections/collectionName {...}) is equivalent to the v1
 * /admin/collections?action=MODIFYCOLLECTION command.
 *
 * @see ModifyCollectionRequestBody
 * @see CollectionApis
 */
public class ModifyCollectionAPI extends AdminAPIBase implements CollectionApis.Modify {

  @Inject
  public ModifyCollectionAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse modifyCollection(
      String collectionName, ModifyCollectionRequestBody requestBody) throws Exception {
    final SubResponseAccumulatingJerseyResponse response =
        instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    ensureRequiredParameterProvided("collectionName", collectionName);
    ensureRequiredRequestBodyProvided(requestBody);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final ZkNodeProps remoteMessage = createRemoteMessage(collectionName, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.MODIFYCOLLECTION,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    processRemoteResponse(response, remoteResponse, requestBody.async);
    return response;
  }

  public static ModifyCollectionRequestBody createV2RequestBody(SolrParams v1Params) {
    // Most parameters handled implicitly here without needing name-change, etc.
    Map<String, Object> v1ParamMap = v1Params.toMap(new HashMap<>());
    v1ParamMap.remove(ACTION);

    // Rename properties that differ b/w v1 and v2
    if (v1ParamMap.containsKey(COLL_CONF)) {
      v1ParamMap.put("config", v1ParamMap.remove(COLL_CONF));
    }

    // Filter out "property." keys into their own submap
    final var arbitraryProperties =
        copyPrefixedPropertiesWithoutPrefix(v1Params, new HashMap<>(), PROPERTY_PREFIX);
    v1ParamMap =
        v1ParamMap.entrySet().stream()
            .filter(e -> !e.getKey().startsWith(PROPERTY_PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Create the POJO and reattach the "property." sub-map
    final var requestBody =
        SolrJacksonMapper.getObjectMapper()
            .convertValue(v1ParamMap, ModifyCollectionRequestBody.class);
    requestBody.properties = arbitraryProperties;

    // Note any properties being "unset" by the v1 params
    requestBody.unset =
        v1Params.stream()
            .filter(entry -> entry.getValue() != null && "".equals(entry.getValue()[0]))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    return requestBody;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, ModifyCollectionRequestBody requestBody) {
    // Check for the properties that Solr allows modification of
    final var messageProps = new HashMap<String, Object>();
    if (requestBody.replicationFactor != null)
      messageProps.put(REPLICATION_FACTOR, requestBody.replicationFactor);
    if (requestBody.config != null) messageProps.put(COLL_CONF, requestBody.config);
    if (requestBody.perReplicaState != null)
      messageProps.put(PER_REPLICA_STATE, requestBody.perReplicaState);
    if (requestBody.readOnly != null) messageProps.put(READ_ONLY, requestBody.readOnly);
    if (requestBody.properties != null && !requestBody.properties.isEmpty()) {
      requestBody
          .properties
          .entrySet()
          .forEach(
              entry -> {
                messageProps.put(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
              });
    }
    // Done to support the v1 capability of "unsetting" properties via an empty-string value
    for (String unsetParam : requestBody.unset) {
      if (MODIFIABLE_COLLECTION_PROPERTIES.contains(unsetParam)) messageProps.put(unsetParam, "");
    }

    if (messageProps.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          formatString(
              "no supported values provided {0}", MODIFIABLE_COLLECTION_PROPERTIES.toString()));
    }

    // Add other props now that we're done "empty-checking"
    messageProps.put(QUEUE_OPERATION, CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower());
    messageProps.put(COLLECTION_PROP, collectionName);
    if (requestBody.async != null) messageProps.put(ASYNC, requestBody.async);

    for (Map.Entry<String, Object> entry : messageProps.entrySet()) {
      String prop = entry.getKey();
      if ("".equals(entry.getValue())) {
        // set to an empty string is equivalent to removing the property, see SOLR-12507
        entry.setValue(null);
      }
      DocCollection.verifyProp(messageProps, prop);
    }
    if (messageProps.get(REPLICATION_FACTOR) != null) {
      messageProps.put(
          Replica.Type.defaultType().numReplicasPropertyName, messageProps.get(REPLICATION_FACTOR));
    }

    return new ZkNodeProps(messageProps);
  }
}
