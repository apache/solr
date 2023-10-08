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
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.COLLECTIONS;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CATEGORY;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CREATE_COLLECTION_PREFIX;
import static org.apache.solr.cloud.api.collections.RoutedAlias.ROUTER_TYPE_NAME;
import static org.apache.solr.cloud.api.collections.RoutedAlias.TIME;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.ROUTER_MAX_FUTURE;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CollectionAdminParams.ROUTER_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.START;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.client.api.model.CreateCollectionRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.RoutedAliasTypes;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.api.collections.RoutedAlias;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.TimeZoneUtils;

/**
 * V2 API for creating an alias
 *
 * <p>This API is analogous to the v1 /admin/collections?action=CREATEALIAS command.
 */
@Path("/aliases")
public class CreateAliasAPI extends AdminAPIBase {
  @Inject
  public CreateAliasAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createAlias(CreateAliasRequestBody requestBody) throws Exception {
    final SubResponseAccumulatingJerseyResponse response =
        instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    if (requestBody == null) {
      throw new SolrException(BAD_REQUEST, "Request body is required but missing");
    }
    requestBody.validate();

    ZkNodeProps remoteMessage;
    // Validation ensures that the request has either collections or a router but not both.
    if (CollectionUtil.isNotEmpty(requestBody.collections)) {
      remoteMessage = createRemoteMessageForTraditionalAlias(requestBody);
    } else { // Creating a routed alias
      assert CollectionUtil.isNotEmpty(requestBody.routers);
      final Aliases aliases = coreContainer.getAliases();
      if (aliases.hasAlias(requestBody.name) && !aliases.isRoutedAlias(requestBody.name)) {
        throw new SolrException(
            BAD_REQUEST,
            "Cannot add routing parameters to existing non-routed Alias: " + requestBody.name);
      }

      remoteMessage = createRemoteMessageForRoutedAlias(requestBody);
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

  public static ZkNodeProps createRemoteMessageForTraditionalAlias(
      CreateAliasRequestBody requestBody) {
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
    if (StrUtils.isNotBlank(requestBody.async)) remoteMessage.put(ASYNC, requestBody.async);

    if (requestBody.routers.size() > 1) { // Multi-dimensional alias
      for (int i = 0; i < requestBody.routers.size(); i++) {
        requestBody.routers.get(i).addRemoteMessageProperties(remoteMessage, "router." + i + ".");
      }
    } else if (requestBody.routers.size() == 1) { // Single dimensional alias
      requestBody.routers.get(0).addRemoteMessageProperties(remoteMessage, "router.");
    }

    if (requestBody.collCreationParameters != null) {
      CreateCollection.addToRemoteMessageWithPrefix(
          requestBody.collCreationParameters, remoteMessage, "create-collection.");
    }
    return new ZkNodeProps(remoteMessage);
  }

  public static CreateAliasRequestBody createFromSolrParams(SolrParams params) {
    final CreateAliasRequestBody createBody = new CreateAliasRequestBody();
    createBody.name = params.required().get(NAME);

    final String collections = params.get(COLLECTIONS);
    createBody.collections =
        StrUtils.isNullOrEmpty(collections) ? new ArrayList<>() : StrUtils.split(collections, ',');
    createBody.async = params.get(ASYNC);

    // Handle routed-alias properties
    final String typeStr = params.get(ROUTER_TYPE_NAME);
    if (typeStr == null) {
      return createBody; // non-routed aliases are being created
    }

    createBody.routers = new ArrayList<>();
    if (typeStr.startsWith(RoutedAlias.DIMENSIONAL)) {
      final String commaSeparatedDimensions =
          typeStr.substring(RoutedAlias.DIMENSIONAL.length(), typeStr.length() - 1);
      final String[] dimensions = commaSeparatedDimensions.split(",");
      if (dimensions.length > 2) {
        throw new SolrException(
            BAD_REQUEST,
            "More than 2 dimensions is not supported yet. "
                + "Please monitor SOLR-13628 for progress");
      }

      for (int i = 0; i < dimensions.length; i++) {
        createBody.routers.add(
            createFromSolrParams(dimensions[i], params, ROUTER_PREFIX + i + "."));
      }
    } else {
      createBody.routers.add(createFromSolrParams(typeStr, params, ROUTER_PREFIX));
    }

    final SolrParams createCollectionParams =
        getHierarchicalParametersByPrefix(params, CREATE_COLLECTION_PREFIX);
    createBody.collCreationParameters =
        CreateCollection.createRequestBodyFromV1Params(createCollectionParams, false);

    return createBody;
  }

  public static RoutedAliasProperties createFromSolrParams(
      String type, SolrParams params, String propertyPrefix) {
    final String typeLower = type.toLowerCase(Locale.ROOT);
    if (typeLower.startsWith(TIME)) {
      return TimeRoutedAliasProperties.createFromSolrParams(params, propertyPrefix);
    } else if (typeLower.startsWith(CATEGORY)) {
      return CategoryRoutedAliasProperties.createFromSolrParams(params, propertyPrefix);
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

    @JsonProperty("collections")
    public List<String> collections;

    @JsonProperty(ASYNC)
    public String async;

    @JsonProperty("routers")
    public List<RoutedAliasProperties> routers;

    @JsonProperty("create-collection")
    public CreateCollectionRequestBody collCreationParameters;

    public void validate() {
      SolrIdentifierValidator.validateAliasName(name);

      if (CollectionUtil.isEmpty(collections) && CollectionUtil.isEmpty(routers)) {
        throw new SolrException(
            BAD_REQUEST,
            "Alias creation requires either a list of either collections (for creating a traditional alias) or routers (for creating a routed alias)");
      }

      if (CollectionUtil.isNotEmpty(routers)) {
        routers.forEach(r -> r.validate());
        if (CollectionUtil.isNotEmpty(collections)) {
          throw new SolrException(
              BAD_REQUEST, "Collections cannot be specified when creating a routed alias.");
        }

        final var createCollReqBody = collCreationParameters;
        if (createCollReqBody != null) {
          if (createCollReqBody.name != null) {
            throw new SolrException(
                BAD_REQUEST,
                "routed aliases calculate names for their "
                    + "dependent collections, you cannot specify the name.");
          }
          if (createCollReqBody.config == null) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Routed alias creation requires a configset name to use for any collections created by the alias.");
          }
        }
      }
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = TimeRoutedAliasProperties.class, name = "time"),
    @JsonSubTypes.Type(value = CategoryRoutedAliasProperties.class, name = "category")
  })
  public abstract static class RoutedAliasProperties implements JacksonReflectMapWriter {
    @JsonProperty(required = true)
    public String field;

    public abstract void validate();

    public abstract void addRemoteMessageProperties(
        Map<String, Object> remoteMessage, String prefix);

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

      // Ensures that provided 'start' and optional 'tz' are of the right format.
      TimeRoutedAlias.parseStringAsInstant(start, TimeZoneUtils.parseTimezone(tz));

      // maxFutureMs must be > 0 if provided
      if (maxFutureMs != null && maxFutureMs < 0) {
        throw new SolrException(BAD_REQUEST, ROUTER_MAX_FUTURE + " must be >= 0");
      }
    }

    @Override
    public void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix) {
      remoteMessage.put(prefix + CoreAdminParams.NAME, TIME);
      remoteMessage.put(prefix + "field", field);
      remoteMessage.put(prefix + "start", start);
      remoteMessage.put(prefix + "interval", interval);

      if (tz != null) remoteMessage.put(prefix + "tz", tz);
      if (maxFutureMs != null) remoteMessage.put(prefix + "maxFutureMs", maxFutureMs);
      if (preemptiveCreateMath != null)
        remoteMessage.put(prefix + "preemptiveCreateMath", preemptiveCreateMath);
      if (autoDeleteAge != null) remoteMessage.put(prefix + "autoDeleteAge", autoDeleteAge);
    }

    public static TimeRoutedAliasProperties createFromSolrParams(
        SolrParams params, String propertyPrefix) {
      final TimeRoutedAliasProperties timeRoutedProperties = new TimeRoutedAliasProperties();
      timeRoutedProperties.field = params.required().get(propertyPrefix + "field");
      timeRoutedProperties.start = params.required().get(propertyPrefix + START);
      timeRoutedProperties.interval = params.required().get(propertyPrefix + "interval");

      timeRoutedProperties.tz = params.get(propertyPrefix + "tz");
      timeRoutedProperties.maxFutureMs = params.getLong(propertyPrefix + "maxFutureMs");
      timeRoutedProperties.preemptiveCreateMath =
          params.get(propertyPrefix + "preemptiveCreateMath");
      timeRoutedProperties.autoDeleteAge = params.get(propertyPrefix + "autoDeleteAge");

      return timeRoutedProperties;
    }
  }

  public static class CategoryRoutedAliasProperties extends RoutedAliasProperties {
    @JsonProperty("maxCardinality")
    public Long maxCardinality;

    @JsonProperty("mustMatch")
    public String mustMatch;

    @Override
    public void validate() {
      ensureRequiredFieldPresent(field, "'field' on category routed alias");
    }

    @Override
    public void addRemoteMessageProperties(Map<String, Object> remoteMessage, String prefix) {
      remoteMessage.put(prefix + CoreAdminParams.NAME, CATEGORY);
      remoteMessage.put(prefix + "field", field);

      if (maxCardinality != null) remoteMessage.put(prefix + "maxCardinality", maxCardinality);
      if (StrUtils.isNotBlank(mustMatch)) remoteMessage.put(prefix + "mustMatch", mustMatch);
    }

    public static CategoryRoutedAliasProperties createFromSolrParams(
        SolrParams params, String propertyPrefix) {
      final CategoryRoutedAliasProperties categoryRoutedProperties =
          new CategoryRoutedAliasProperties();
      categoryRoutedProperties.field = params.required().get(propertyPrefix + "field");

      categoryRoutedProperties.maxCardinality = params.getLong(propertyPrefix + "maxCardinality");
      categoryRoutedProperties.mustMatch = params.get(propertyPrefix + "mustMatch");
      return categoryRoutedProperties;
    }
  }

  /**
   * Returns a SolrParams object containing only those values whose keys match a specified prefix
   * (with that prefix removed)
   *
   * <p>Query-parameter based v1 APIs often mimic hierarchical parameters by using a prefix in the
   * query-param key to group similar parameters together. This function can be used to identify all
   * of the parameters "nested" in this way, with their prefix removed.
   */
  public static SolrParams getHierarchicalParametersByPrefix(
      SolrParams paramSource, String prefix) {
    final ModifiableSolrParams filteredParams = new ModifiableSolrParams();
    paramSource.stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .forEach(e -> filteredParams.add(e.getKey().substring(prefix.length()), e.getValue()));
    return filteredParams;
  }
}
