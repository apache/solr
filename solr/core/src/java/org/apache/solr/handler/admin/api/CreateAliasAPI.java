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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.beans.V2ApiConstants;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.jersey.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.ROUTER_KEY;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET_SHUFFLE;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.NUM_SLICES;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARDS_PROP;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CREATE_COLLECTION_PREFIX;
import static org.apache.solr.cloud.api.collections.RoutedAlias.ROUTER_TYPE_NAME;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CollectionAdminParams.ALIAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.NRT_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.PER_REPLICA_STATE;
import static org.apache.solr.common.params.CollectionAdminParams.PULL_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.TLOG_REPLICAS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.START;
import static org.apache.solr.common.params.CoreAdminParams.CONFIG;
import static org.apache.solr.common.params.CoreAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

@Path("/aliases")
public class CreateAliasAPI extends AdminAPIBase {
    @Inject
    public CreateAliasAPI(CoreContainer coreContainer, SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
        super(coreContainer, solrQueryRequest, solrQueryResponse);
    }

    @POST
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
    @PermissionName(COLL_EDIT_PERM)
    public SolrJerseyResponse createAlias(CreateAliasRequestBody requestBody) throws Exception {
        final SubResponseAccumulatingJerseyResponse response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
        recordCollectionForLogAndTracing(null, solrQueryRequest);
        validateRequestBody(requestBody);

        ZkNodeProps remoteMessage;
        // Validation ensures that the request has either collections or a router but not both.
        if (CollectionUtils.isNotEmpty(requestBody.collections)) {
            remoteMessage = createRemoteMessageForTraditionalAlias(requestBody);
        } else { // Creating a routed alias
            assert CollectionUtils.isNotEmpty(requestBody.routers);
            final Aliases aliases = coreContainer.getAliases();
            if (aliases.hasAlias(requestBody.name) && !aliases.isRoutedAlias(requestBody.name)) {
                throw new SolrException(
                        BAD_REQUEST,
                        "Cannot add routing parameters to existing non-routed Alias: " + requestBody.name);
            }

            remoteMessage = createRemoteMessageForRoutedAlias(requestBody);
            requestBody.collCreationParameters.name = "TMP_name_TMP_name_TMP";
            // TODO Create the placeholder collection
            //           CREATE_OP.execute(
            //              new LocalSolrQueryRequest(null, createCollParams), rsp, h); // ignore results
        }

        final SolrResponse remoteResponse =
                CollectionsHandler.submitCollectionApiCommand(
                        coreContainer,
                        coreContainer.getDistributedCollectionCommandRunner(),
                        remoteMessage,
                        CollectionParams.CollectionAction.CREATEALIAS,
                        DEFAULT_COLLECTION_OP_TIMEOUT);
        if (remoteResponse.getException() != null) {
            throw remoteResponse.getException();
        }

        if (requestBody.async != null) {
            response.requestId = requestBody.async;
        }
        return response;
    }

    public static ZkNodeProps createRemoteMessageForTraditionalAlias(CreateAliasRequestBody requestBody) {
        final Map<String, Object> remoteMessage = new HashMap<>();

        remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATEALIAS.toLower());
        remoteMessage.put(NAME, requestBody.name);
        remoteMessage.put("collections", String.join(",", requestBody.collections));
        remoteMessage.put(ASYNC, requestBody.async);

        return new ZkNodeProps(remoteMessage);
    }

    public static ZkNodeProps createRemoteMessageForRoutedAlias(CreateAliasRequestBody requestBody) {
        final Map<String, Object> remoteMessage = new HashMap<>();
        remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATEALIAS.toLower());
        remoteMessage.put(NAME, requestBody.name);
        remoteMessage.put(ASYNC, requestBody.async);

        if (requestBody.routers.size() > 1) { // Multi-dimensional alias
            for (int i = 0; i < requestBody.routers.size(); i++) {
                requestBody.routers.get(i).addRemoteMessageProperties(remoteMessage, "router." + i + ".");
            }
        } else if (requestBody.routers.size() == 1) { // Single dimensional alias
            requestBody.routers.get(0).addRemoteMessageProperties(remoteMessage, "router.");
        }

        if (requestBody.collCreationParameters != null) {
            requestBody.collCreationParameters.addRemoteMessageProperties(remoteMessage, "create-collection.");
        }
        return new ZkNodeProps(remoteMessage);
    }

    public static void validateRequestBody(CreateAliasRequestBody requestBody) {
        if (requestBody == null) {
            return;
        }

        SolrIdentifierValidator.validateAliasName(requestBody.name);

        if (CollectionUtils.isEmpty(requestBody.collections) && CollectionUtils.isEmpty(requestBody.routers)) {
            throw new SolrException(BAD_REQUEST, "Alias creation requires either a list of either collections (for creating a traditional alias) or routers (for creating a routed alias)");
        }

        if (CollectionUtils.isNotEmpty(requestBody.routers)) {
            requestBody.routers.forEach(r -> r.validate());
            if (CollectionUtils.isNotEmpty(requestBody.collections)) {
                throw new SolrException(
                        BAD_REQUEST, "Collections cannot be specified when creating a routed alias.");
            }

            final CreateCollectionRequestBody createCollReqBody = requestBody.collCreationParameters;
            if (createCollReqBody != null) {
                if (createCollReqBody.name != null) {
                    throw new SolrException(
                            BAD_REQUEST,
                            "routed aliases calculate names for their "
                                    + "dependent collections, you cannot specify the name.");
                }
                if (createCollReqBody.config == null) {
                    throw new SolrException(
                            SolrException.ErrorCode.BAD_REQUEST, "We require an explicit " + COLL_CONF);
                }
            }
        }
    }

    public static CreateAliasRequestBody createFromSolrParams(SolrParams params) {
        final CreateAliasRequestBody createBody = new CreateAliasRequestBody();
        createBody.name = params.required().get(NAME);

        final String collections = params.get("collections");
        createBody.collections = StringUtils.isEmpty(collections) ? new ArrayList<>() : Arrays.asList(StringUtils.split(collections, ','));
        createBody.async = params.get(ASYNC);

        // Handle routed-alias properties
        final String typeStr = params.get(ROUTER_TYPE_NAME);
        if (typeStr == null) {
            return createBody; // non-routed aliases are being created
        }

        createBody.routers = new ArrayList<>();
        // TODO NOCOMMIT constants for these values
        if (typeStr.startsWith("Dimensional[")) {
            final String commaSeparatedDimensions = typeStr.substring("Dimensional[".length(), typeStr.length() - 1);
            final String[] dimensions = commaSeparatedDimensions.split(",");
            if (dimensions.length > 2) {
                throw new SolrException(
                        BAD_REQUEST,
                        "More than 2 dimensions is not supported yet. "
                                + "Please monitor SOLR-13628 for progress");
            }

            for (int i = 0; i < dimensions.length; i++) {
                createBody.routers.add(createFromSolrParams(dimensions[i], params, "router." + i + "."));
            }
        } else {
            createBody.routers.add(createFromSolrParams(typeStr, params, "router."));
        }

        final SolrParams createCollectionParams = getHierarchicalParametersByPrefix(params, CREATE_COLLECTION_PREFIX);
        buildRequestBodyFromParams(createCollectionParams);

        return createBody;
    }

    public static RoutedAliasProperties createFromSolrParams(String type, SolrParams params, String propertyPrefix) {
        if (type.startsWith("time")) {
            return TimeRoutedAliasProperties.createFromSolrParams(params, "router.");
        } else if (type.startsWith("category")) {
            return CategoryRoutedAliasProperties.createFromSolrParams(params, "router.");
        } else {
            throw new SolrException(
                    BAD_REQUEST,
                    "Router name: "
                            + type
                            + " is not in supported types, "
                            + Arrays.asList(RoutedAliasTypes.values()));
        }
    }


    public static class CreateAliasRequestBody implements JacksonReflectMapWriter {
        @JsonProperty(required = true)
        public String name;

        // TODO v1 takes this in as a comma-delimited string
        @JsonProperty("collections")
        public List<String> collections;

        @JsonProperty(ASYNC)
        public String async;

        @JsonProperty("routers")
        public List<RoutedAliasProperties> routers;

        @JsonProperty("create-collection")
        public CreateCollectionRequestBody collCreationParameters;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = TimeRoutedAliasProperties.class, name = "time"),
            @JsonSubTypes.Type(value = CategoryRoutedAliasProperties.class, name = "category")
    })
    public static abstract class RoutedAliasProperties implements JacksonReflectMapWriter {
        @JsonProperty(required = true)
        public String field;

        public abstract void validate();

        public abstract void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix);

        protected void ensureRequiredFieldPresent(Object val, String name) {
            if (val == null) {
                throw new SolrException(
                        SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: " + name);
            }
        }
    }

    public static class TimeRoutedAliasProperties extends RoutedAliasProperties {
        // Expected to be a date/time in ISO format, or 'NOW'
        @JsonProperty(required = true)
        public String start;

        // TODO Change this to 'timezone' or something less abbreviated
        @JsonProperty("tz")
        public String tz;

        @JsonProperty(required = true)
        public String interval;

        @JsonProperty("maxFutureMs")
        public Long maxFutureMs;

        @JsonProperty("preemptiveCreateMath")
        public String preemptiveCreateMath;

        @JsonProperty("autoDeleteAge")
        public String autoDeleteAge;

        @Override
        public void validate() {
            ensureRequiredFieldPresent(field, "'field' on time routed alias");
            ensureRequiredFieldPresent(start, "'start' on time routed alias");
            ensureRequiredFieldPresent(interval, "'interval' on time routed alias");
        }

        @Override
        public void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix) {
            remoteMessage.put(prefix + CoreAdminParams.NAME, "time");
            remoteMessage.put(prefix + "field", field);
            remoteMessage.put(prefix + "start", start);
            remoteMessage.put(prefix + "interval", interval);

            remoteMessage.put(prefix + "tz", tz);
            remoteMessage.put(prefix + "maxFutureMs", maxFutureMs);
            remoteMessage.put(prefix + "preemptiveCreateMath", preemptiveCreateMath);
            remoteMessage.put(prefix + "autoDeleteAge", autoDeleteAge);
        }

        public static TimeRoutedAliasProperties createFromSolrParams(SolrParams params, String propertyPrefix) {
            final TimeRoutedAliasProperties timeRoutedProperties = new TimeRoutedAliasProperties();
            timeRoutedProperties.field = params.required().get(propertyPrefix + "field");
            timeRoutedProperties.start = params.required().get(propertyPrefix + START);
            timeRoutedProperties.interval = params.required().get(propertyPrefix + "interval");

            timeRoutedProperties.tz = params.get(propertyPrefix + "tz");
            timeRoutedProperties.maxFutureMs = params.getLong(propertyPrefix + "maxFutureMs");
            timeRoutedProperties.preemptiveCreateMath = params.get(propertyPrefix + "preemptiveCreateMath");
            timeRoutedProperties.autoDeleteAge = params.get(propertyPrefix + "autoDeleteAge");

            return timeRoutedProperties;
        }
    }

    public static class CategoryRoutedAliasProperties extends RoutedAliasProperties {
        @JsonProperty("maxCardinality")
        public Long maxCardinality;

        @JsonProperty("mustMatch")
        public String mustMatch;

        public void validate() {
            ensureRequiredFieldPresent(field, "'field' on category routed alias");
        }

        @Override
        public void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix) {
            remoteMessage.put(prefix + CoreAdminParams.NAME, "time");
            remoteMessage.put(prefix + "field", field);

            remoteMessage.put(prefix + "maxCardinality", maxCardinality);
            remoteMessage.put(prefix + "mustMatch", mustMatch);
        }

        public static CategoryRoutedAliasProperties createFromSolrParams(SolrParams params, String propertyPrefix) {
            final CategoryRoutedAliasProperties categoryRoutedProperties = new CategoryRoutedAliasProperties();
            categoryRoutedProperties.field = params.required().get(propertyPrefix + "field");

            categoryRoutedProperties.maxCardinality = params.getLong(propertyPrefix + "maxCardinality");
            categoryRoutedProperties.mustMatch = params.get(propertyPrefix + "mustMatch");
            return categoryRoutedProperties;
        }
    }

    // TODO Move this to CreateCollectionAPI when that is created.
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

        // TODO This is currently a comma-separate list of shard names.  We should change it to be an actual List<String> instead, and maybe rename to 'shardNames' or something similar
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
        public CollectionRouterProperties router;

        public void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix) {
            final Map<String, Object> v1Params = toMap(new HashMap<>());
            convertV2CreateCollectionMapToV1ParamMap(v1Params);
            for (Map.Entry<String, Object> v1Param : v1Params.entrySet()) {
                remoteMessage.put(prefix + v1Param.getKey(), v1Param.getValue());
            }
        }

        private void convertV2CreateCollectionMapToV1ParamMap(Map<String, Object> v2MapVals) {
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

    public static CreateCollectionRequestBody buildRequestBodyFromParams(SolrParams params) {
        final CreateCollectionRequestBody requestBody = new CreateCollectionRequestBody();
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
        requestBody.properties = copyPrefixedPropertiesWithoutPrefix(params, new HashMap<>(), PROPERTY_PREFIX);
        if (params.get("router.name") != null || params.get("router.field") != null) {
            final CollectionRouterProperties routerProperties = new CollectionRouterProperties();
            routerProperties.name = params.get("router.name");
            routerProperties.field = params.get("router.field");
            requestBody.router = routerProperties;
        }

        return requestBody;
    }

    /**
     * Returns a SolrParams object containing only those values whose keys match a specified prefix (with that prefix removed)
     *
     * Query-parameter based v1 APIs often mimic hierarchical parameters by using a prefix in
     * the query-param key to group similar parameters together.  This function can be used to
     * identify all of the parameters "nested" in this way, with their prefix removed.
     */
    public static SolrParams getHierarchicalParametersByPrefix(SolrParams paramSource, String prefix) {
        final ModifiableSolrParams filteredParams = new ModifiableSolrParams();
        paramSource.stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .forEach(e -> filteredParams.add(e.getKey().substring(prefix.length()), e.getValue()));
        return filteredParams;
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

    public static class CollectionRouterProperties implements JacksonReflectMapWriter {
        @JsonProperty(NAME)
        public String name;

        @JsonProperty("field")
        public String field;
    }
}
