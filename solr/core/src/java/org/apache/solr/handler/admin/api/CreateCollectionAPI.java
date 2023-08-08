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
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.ROUTER_KEY;
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.SHARD_NAMES;
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
import static org.apache.solr.handler.admin.CollectionsHandler.waitForActiveCollection;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.schema.IndexSchema.FIELD;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.beans.V2ApiConstants;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * V2 API for creating a SolrCLoud collection
 *
 * <p>This API is analogous to the v1 /admin/collections?action=CREATE command.
 */
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

    if (requestBody == null) {
      throw new SolrException(BAD_REQUEST, "Request body is missing but required");
    }

    final SubResponseAccumulatingJerseyResponse response =
        instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(requestBody.name, solrQueryRequest);

    // We must always create a .system collection with only a single shard
    if (CollectionAdminParams.SYSTEM_COLL.equals(requestBody.name)) {
      requestBody.numShards = 1;
      requestBody.shardNames = null;
      createSysConfigSet(coreContainer);
    }

    requestBody.validate();

    // Populate any 'null' creation parameters that support COLLECTIONPROP defaults.
    populateDefaultsIfNecessary(coreContainer, requestBody);

    final ZkNodeProps remoteMessage = createRemoteMessage(requestBody);
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
    response.warning = (String) remoteResponse.getResponse().get("warning");

    // Even if Overseer does wait for the collection to be created, it sees a different cluster
    // state than this node, so this wait is required to make sure the local node Zookeeper watches
    // fired and now see the collection.
    if (requestBody.async == null) {
      waitForActiveCollection(requestBody.name, coreContainer, remoteResponse);
    }

    return response;
  }

  public static void populateDefaultsIfNecessary(
      CoreContainer coreContainer, CreateCollectionRequestBody requestBody) throws IOException {
    if (CollectionUtil.isEmpty(requestBody.shardNames) && requestBody.numShards == null) {
      requestBody.numShards = readIntegerDefaultFromClusterProp(coreContainer, NUM_SLICES);
    }
    if (requestBody.nrtReplicas == null)
      requestBody.nrtReplicas = readIntegerDefaultFromClusterProp(coreContainer, NRT_REPLICAS);
    if (requestBody.tlogReplicas == null)
      requestBody.tlogReplicas = readIntegerDefaultFromClusterProp(coreContainer, TLOG_REPLICAS);
    if (requestBody.pullReplicas == null)
      requestBody.pullReplicas = readIntegerDefaultFromClusterProp(coreContainer, PULL_REPLICAS);
  }

  private static void verifyShardsParam(List<String> shardNames) {
    for (String shard : shardNames) {
      SolrIdentifierValidator.validateShardName(shard);
    }
  }

  public static ZkNodeProps createRemoteMessage(CreateCollectionRequestBody reqBody) {
    final Map<String, Object> rawProperties = new HashMap<>();
    rawProperties.put("fromApi", "true");

    rawProperties.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower());
    rawProperties.put(NAME, reqBody.name);
    rawProperties.put(COLL_CONF, reqBody.config);
    rawProperties.put(NUM_SLICES, reqBody.numShards);
    if (reqBody.shuffleNodes != null)
      rawProperties.put(CREATE_NODE_SET_SHUFFLE, reqBody.shuffleNodes);
    if (CollectionUtil.isNotEmpty(reqBody.shardNames))
      rawProperties.put(SHARDS_PROP, String.join(",", reqBody.shardNames));
    rawProperties.put(PULL_REPLICAS, reqBody.pullReplicas);
    rawProperties.put(TLOG_REPLICAS, reqBody.tlogReplicas);
    rawProperties.put(WAIT_FOR_FINAL_STATE, reqBody.waitForFinalState);
    rawProperties.put(PER_REPLICA_STATE, reqBody.perReplicaState);
    rawProperties.put(ALIAS, reqBody.alias);
    rawProperties.put(ASYNC, reqBody.async);
    if (reqBody.createReplicas == null || reqBody.createReplicas) {
      // The remote message expects a single comma-delimited string, so nodeSet requires flattening
      if (reqBody.nodeSet != null) {
        rawProperties.put(CREATE_NODE_SET, String.join(",", reqBody.nodeSet));
      }
    } else {
      rawProperties.put(CREATE_NODE_SET, "EMPTY");
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

  private static Integer readIntegerDefaultFromClusterProp(
      CoreContainer coreContainer, String propName) throws IOException {
    final Object defaultValue =
        new ClusterProperties(coreContainer.getZkController().getZkStateReader().getZkClient())
            .getClusterProperty(
                List.of(CollectionAdminParams.DEFAULTS, CollectionAdminParams.COLLECTION, propName),
                null);
    if (defaultValue == null) return null;

    return Integer.valueOf(String.valueOf(defaultValue));
  }

  private static void createSysConfigSet(CoreContainer coreContainer)
      throws KeeperException, InterruptedException {
    SolrZkClient zk = coreContainer.getZkController().getZkStateReader().getZkClient();
    ZkMaintenanceUtils.ensureExists(ZkStateReader.CONFIGS_ZKNODE, zk);
    ZkMaintenanceUtils.ensureExists(
        ZkStateReader.CONFIGS_ZKNODE + "/" + CollectionAdminParams.SYSTEM_COLL, zk);

    try {
      String path =
          ZkStateReader.CONFIGS_ZKNODE + "/" + CollectionAdminParams.SYSTEM_COLL + "/schema.xml";
      byte[] data;
      try (InputStream inputStream =
          CollectionsHandler.class.getResourceAsStream("/SystemCollectionSchema.xml")) {
        assert inputStream != null;
        data = inputStream.readAllBytes();
      }
      assert data != null && data.length > 0;
      ZkMaintenanceUtils.ensureExists(path, data, CreateMode.PERSISTENT, zk);
      path =
          ZkStateReader.CONFIGS_ZKNODE
              + "/"
              + CollectionAdminParams.SYSTEM_COLL
              + "/solrconfig.xml";
      try (InputStream inputStream =
          CollectionsHandler.class.getResourceAsStream("/SystemCollectionSolrConfig.xml")) {
        assert inputStream != null;
        data = inputStream.readAllBytes();
      }
      assert data != null && data.length > 0;
      ZkMaintenanceUtils.ensureExists(path, data, CreateMode.PERSISTENT, zk);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /** Request body for v2 "create collection" requests */
  public static class CreateCollectionRequestBody implements JacksonReflectMapWriter {
    @JsonProperty(NAME)
    public String name;

    @JsonProperty(REPLICATION_FACTOR)
    public Integer replicationFactor;

    @JsonProperty(CONFIG)
    public String config;

    @JsonProperty(NUM_SLICES)
    public Integer numShards;

    @JsonProperty(SHARD_NAMES)
    public List<String> shardNames;

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
    public String alias;

    @JsonProperty("properties")
    public Map<String, String> properties;

    @JsonProperty(ASYNC)
    public String async;

    @JsonProperty("router")
    public RouterProperties router;

    // Parameters below differ from v1 API
    // V1 API uses createNodeSet
    @JsonProperty("nodeSet")
    public List<String> nodeSet;
    // v1 API uses createNodeSet=EMPTY
    @JsonProperty("createReplicas")
    public Boolean createReplicas;
    // V1 API uses 'createNodeSet.shuffle'
    @JsonProperty("shuffleNodes")
    public Boolean shuffleNodes;

    public static CreateCollectionRequestBody fromV1Params(
        SolrParams params, boolean nameRequired) {
      final var requestBody = new CreateCollectionRequestBody();
      requestBody.name =
          nameRequired ? params.required().get(CommonParams.NAME) : params.get(CommonParams.NAME);
      requestBody.replicationFactor = params.getInt(ZkStateReader.REPLICATION_FACTOR);
      requestBody.config = params.get(COLL_CONF);
      requestBody.numShards = params.getInt(NUM_SLICES);
      if (params.get(CREATE_NODE_SET) != null) {
        final String commaDelimNodeSet = params.get(CREATE_NODE_SET);
        if ("EMPTY".equals(commaDelimNodeSet)) {
          requestBody.createReplicas = false;
        } else {
          requestBody.nodeSet = Arrays.asList(params.get(CREATE_NODE_SET).split(","));
        }
      }
      requestBody.shuffleNodes = params.getBool(CREATE_NODE_SET_SHUFFLE);
      requestBody.shardNames =
          params.get(SHARDS_PROP) != null
              ? Arrays.stream(params.get(SHARDS_PROP).split(",")).collect(Collectors.toList())
              : new ArrayList<>();
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
        final RouterProperties routerProperties = new RouterProperties();
        routerProperties.name = params.get("router.name");
        routerProperties.field = params.get("router.field");
        requestBody.router = routerProperties;
      }

      return requestBody;
    }

    public void validate() {
      if (replicationFactor != null
          && nrtReplicas != null
          && (!replicationFactor.equals(nrtReplicas))) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Cannot specify both replicationFactor and nrtReplicas as they mean the same thing");
      }

      SolrIdentifierValidator.validateCollectionName(name);

      if (shardNames != null && !shardNames.isEmpty()) {
        verifyShardsParam(shardNames);
      }
    }

    public void addToRemoteMessageWithPrefix(Map<String, Object> remoteMessage, String prefix) {
      final Map<String, Object> v1Params = toMap(new HashMap<>());
      convertV2CreateCollectionMapToV1ParamMap(v1Params);
      for (Map.Entry<String, Object> v1Param : v1Params.entrySet()) {
        remoteMessage.put(prefix + v1Param.getKey(), v1Param.getValue());
      }
    }

    /**
     * Convert a map representing the v2 request body into v1-appropriate query-parameters.
     *
     * <p>Most v2 APIs using the legacy (i.e. non-JAX-RS) framework implement the v2 API by
     * restructuring the provided parameters so that the v1 codepath can be called. This utility
     * method is provided in pursuit of that usecase. It's not used directly CreateCollectionAPI,
     * which uses the JAX-RS framework, but it's kept here so that logic surrounding
     * collection-creation parameters can be kept in a single place.
     */
    @SuppressWarnings("unchecked")
    public static void convertV2CreateCollectionMapToV1ParamMap(Map<String, Object> v2MapVals) {
      // Keys are copied so that map can be modified as keys are looped through.
      final Set<String> v2Keys = v2MapVals.keySet().stream().collect(Collectors.toSet());
      for (String key : v2Keys) {
        switch (key) {
          case V2ApiConstants.PROPERTIES_KEY:
            final Map<String, Object> propertiesMap =
                (Map<String, Object>) v2MapVals.remove(V2ApiConstants.PROPERTIES_KEY);
            flattenMapWithPrefix(propertiesMap, v2MapVals, CollectionAdminParams.PROPERTY_PREFIX);
            break;
          case ROUTER_KEY:
            final Map<String, Object> routerProperties =
                (Map<String, Object>) v2MapVals.remove(V2ApiConstants.ROUTER_KEY);
            flattenMapWithPrefix(routerProperties, v2MapVals, CollectionAdminParams.ROUTER_PREFIX);
            break;
          case V2ApiConstants.CONFIG:
            v2MapVals.put(CollectionAdminParams.COLL_CONF, v2MapVals.remove(V2ApiConstants.CONFIG));
            break;
          case SHARD_NAMES:
            final String shardsValue =
                String.join(",", (Collection<String>) v2MapVals.remove(SHARD_NAMES));
            v2MapVals.put(SHARDS_PROP, shardsValue);
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

  public static class RouterProperties implements JacksonReflectMapWriter {
    @JsonProperty(NAME)
    public String name;

    @JsonProperty(FIELD)
    public String field;
  }
}
