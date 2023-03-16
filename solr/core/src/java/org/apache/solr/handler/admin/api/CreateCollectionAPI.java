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
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET_SHUFFLE;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.NUM_SLICES;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARDS_PROP;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CollectionAdminParams.ALIAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.NRT_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.PER_REPLICA_STATE;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.PULL_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.TLOG_REPLICAS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CoreAdminParams.CONFIG;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.schema.IndexSchema.FIELD;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

@Path("/collections")
public class CreateCollectionAPI extends AdminAPIBase {

  @Inject
  public CreateCollectionAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse createCollection(
      CreateCollectionRequestBody requestBody) throws Exception {
    final SubResponseAccumulatingJerseyResponse response =
        instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(requestBody.name, solrQueryRequest);

    // We must always create a .system collection with only a single shard
    if (CollectionAdminParams.SYSTEM_COLL.equals(requestBody.name)) {
      requestBody.numShards = 1;
      requestBody.shards = null;
      CollectionsHandler.createSysConfigSet(coreContainer);
    }

    validateRequestBody(requestBody);
    final ZkNodeProps remoteMessage = createRemoteMessage(coreContainer, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.CREATE,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    if (requestBody.async != null) {
      response.requestId = requestBody.async;
      return response;
    }

    // Values fetched from remoteResponse may be null
    response.successfulSubResponsesByNodeName = remoteResponse.getResponse().get("success");
    response.failedSubResponsesByNodeName = remoteResponse.getResponse().get("failure");

    return response;
  }

  public static void validateRequestBody(CreateCollectionRequestBody reqBody) {
    if (reqBody.replicationFactor != null
        && reqBody.nrtReplicas != null
        && reqBody.replicationFactor != reqBody.nrtReplicas) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Cannot specify both replicationFactor and nrtReplicas as they mean the same thing");
    }

    SolrIdentifierValidator.validateCollectionName(reqBody.name);

    if (StringUtils.isNotEmpty(reqBody.shards)) {
      verifyShardsParam(reqBody.shards);
    }
  }

  private static void verifyShardsParam(String shardsParam) {
    for (String shard : shardsParam.split(",")) {
      SolrIdentifierValidator.validateShardName(shard);
    }
  }

  public static ZkNodeProps createRemoteMessage(
      CoreContainer coreContainer, CreateCollectionRequestBody reqBody) throws IOException {
    final Map<String, Object> rawProperties = new HashMap<>();
    rawProperties.put("fromApi", "true"); // TODO NOCOMMIT Wtf is this for?

    rawProperties.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower());
    rawProperties.put(NAME, reqBody.name);
    rawProperties.put(COLL_CONF, reqBody.config);
    rawProperties.put(NUM_SLICES, reqBody.numShards);
    rawProperties.put(CREATE_NODE_SET_SHUFFLE, reqBody.shuffleNodes);
    rawProperties.put(SHARDS_PROP, reqBody.shards);
    rawProperties.put(PULL_REPLICAS, reqBody.pullReplicas);
    rawProperties.put(TLOG_REPLICAS, reqBody.tlogReplicas);
    rawProperties.put(WAIT_FOR_FINAL_STATE, reqBody.waitForFinalState);
    rawProperties.put(PER_REPLICA_STATE, reqBody.perReplicaState);
    rawProperties.put(ALIAS, reqBody.alias);
    rawProperties.put(ASYNC, reqBody.async);
    // The remote message expects a single comma-delimited string, so nodeSet requires flattening
    if (reqBody.nodeSet != null) {
      rawProperties.put(CREATE_NODE_SET, String.join(",", reqBody.nodeSet));
    }
    // 'nrtReplicas' and 'replicationFactor' are both set on the remote message, despite being
    // functionally equivalent.
    if (reqBody.replicationFactor != null) {
      rawProperties.put(REPLICATION_FACTOR, reqBody.replicationFactor);
      if (reqBody.nrtReplicas == null) rawProperties.put(NRT_REPLICAS, reqBody.replicationFactor);
    }
    if (reqBody.nrtReplicas != null) {
      rawProperties.put(NRT_REPLICAS, reqBody.nrtReplicas);
      if (reqBody.replicationFactor == null)
        rawProperties.put(REPLICATION_FACTOR, reqBody.nrtReplicas);
    }

    // TODO Separate this out so it happens prior to message creation?  Might make it easier to
    // test...
    // If needed, populate any 'null' creation parameters that support COLLECTIONPROP defaults.
    if (reqBody.shards == null) {
      copyFromClusterProp(coreContainer, rawProperties, NUM_SLICES);
    }
    for (String prop : Set.of(NRT_REPLICAS, PULL_REPLICAS, TLOG_REPLICAS)) {
      copyFromClusterProp(coreContainer, rawProperties, prop);
    }

    if (reqBody.properties != null) {
      for (Map.Entry<String, String> entry : reqBody.properties.entrySet()) {
        rawProperties.put(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
      }
    }

    if (reqBody.router != null) {
      final RouterProperties routerProps = reqBody.router;
      rawProperties.put("router.name", routerProps.name);
      rawProperties.put("router.field", routerProps.field);
    }

    return new ZkNodeProps(rawProperties);
  }

  public static CreateCollectionRequestBody buildRequestBodyFromParams(SolrParams params) {
    final CreateCollectionAPI.CreateCollectionRequestBody requestBody =
        new CreateCollectionAPI.CreateCollectionRequestBody();
    requestBody.name = params.required().get(CommonParams.NAME);
    requestBody.replicationFactor = params.getInt(ZkStateReader.REPLICATION_FACTOR);
    requestBody.config = params.get(COLL_CONF);
    requestBody.numShards = params.getInt(NUM_SLICES);
    if (params.get(CREATE_NODE_SET) != null) {
      requestBody.nodeSet = Arrays.asList(params.get(CREATE_NODE_SET).split(","));
    }
    requestBody.shuffleNodes = params.getBool(CREATE_NODE_SET_SHUFFLE);
    requestBody.shards = params.get(SHARDS_PROP);
    requestBody.tlogReplicas = params.getInt(ZkStateReader.TLOG_REPLICAS);
    requestBody.pullReplicas = params.getInt(ZkStateReader.PULL_REPLICAS);
    requestBody.nrtReplicas = params.getInt(ZkStateReader.NRT_REPLICAS);
    requestBody.waitForFinalState = params.getBool(WAIT_FOR_FINAL_STATE);
    requestBody.perReplicaState = params.getBool(PER_REPLICA_STATE);
    requestBody.alias = params.get(ALIAS);
    requestBody.async = params.get(ASYNC);
    requestBody.properties =
        copyPrefixedPropertiesWithoutPrefix(params, new HashMap<>(), PROPERTY_PREFIX);
    if (params.get("router.name") != null || params.get("router.field") != null) {
      final CreateCollectionAPI.RouterProperties routerProperties =
          new CreateCollectionAPI.RouterProperties();
      routerProperties.name = params.get("router.name");
      routerProperties.field = params.get("router.field");
      requestBody.router = routerProperties;
    }

    return requestBody;
  }

  public static Map<String, String> copyPrefixedPropertiesWithoutPrefix(
      SolrParams params, Map<String, String> props, String prefix) {
    Iterator<String> iter = params.getParameterNamesIterator();
    while (iter.hasNext()) {
      String param = iter.next();
      if (param.startsWith(prefix)) {
        final String[] values = params.getParams(param);
        if (values.length != 1) {
          throw new SolrException(
              BAD_REQUEST, "Only one value can be present for parameter " + param);
        }
        final String modifiedKey = param.replaceFirst(prefix, "");
        props.put(modifiedKey, values[0]);
      }
    }
    return props;
  }

  private static void copyFromClusterProp(
      CoreContainer coreContainer, Map<String, Object> props, String prop) throws IOException {
    if (props.get(prop) != null) return; // if it's already specified , return
    Object defVal =
        new ClusterProperties(coreContainer.getZkController().getZkStateReader().getZkClient())
            .getClusterProperty(
                List.of(CollectionAdminParams.DEFAULTS, CollectionAdminParams.COLLECTION, prop),
                null);
    if (defVal != null) props.put(prop, String.valueOf(defVal));
  }

  public static class CreateCollectionRequestBody implements JacksonReflectMapWriter {
    @JsonProperty(NAME)
    public String name;

    @JsonProperty(REPLICATION_FACTOR)
    public Integer replicationFactor;

    @JsonProperty(CONFIG)
    public String config;

    @JsonProperty(NUM_SLICES)
    public Integer numShards;

    @JsonProperty("nodeSet") // V1 API uses 'createNodeSet'
    public List<String> nodeSet;

    @JsonProperty("shuffleNodes") // V1 API uses 'createNodeSet.shuffle'
    public Boolean shuffleNodes;

    // TODO This is currently a comma-separate list of shard names.  We should change it to be an
    // actual List<String> instead, and maybe rename to 'shardNames' or something similar
    @JsonProperty(SHARDS_PROP)
    public String shards;

    @JsonProperty(PULL_REPLICAS)
    public Integer pullReplicas;

    @JsonProperty(TLOG_REPLICAS)
    public Integer tlogReplicas;

    @JsonProperty(NRT_REPLICAS)
    public Integer nrtReplicas;

    @JsonProperty(WAIT_FOR_FINAL_STATE)
    public Boolean waitForFinalState;

    @JsonProperty(PER_REPLICA_STATE)
    public Boolean perReplicaState;

    @JsonProperty(ALIAS)
    public String alias; // the name of an alias to create in addition to the collection

    @JsonProperty("properties")
    public Map<String, String> properties;

    @JsonProperty(ASYNC)
    public String async;

    @JsonProperty("router")
    public RouterProperties router;
  }

  public static class RouterProperties implements JacksonReflectMapWriter {
    @JsonProperty(NAME)
    public String name;

    @JsonProperty(FIELD)
    public String field;
  }
}
