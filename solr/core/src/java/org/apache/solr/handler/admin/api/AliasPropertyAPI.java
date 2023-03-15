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
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 APIs for managing and inspecting properties for collection aliases */
@Path("/aliases/{aliasName}/properties")
public class AliasPropertyAPI extends AdminAPIBase {

  @Inject
  public AliasPropertyAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @GET
  @PermissionName(COLL_READ_PERM)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @Operation(
      summary = "Get properties for a collection alias.",
      tags = {"aliases"})
  public GetAllAliasPropertiesResponse getAllAliasProperties(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName)
      throws Exception {
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    final GetAllAliasPropertiesResponse response =
        instantiateJerseyResponse(GetAllAliasPropertiesResponse.class);
    final Aliases aliases = readAliasesFromZk();
    if (aliases != null) {
      response.properties = aliases.getCollectionAliasProperties(aliasName);
    } else {
      throw new SolrException(SolrException.ErrorCode.NOT_FOUND, aliasName + " not found");
    }

    return response;
  }

  @GET
  @Path("/{propName}")
  @PermissionName(COLL_READ_PERM)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @Operation(
      summary = "Get a specific property for a collection alias.",
      tags = {"aliases"})
  public GetAliasPropertyResponse getAliasProperty(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @Parameter(description = "Property Name") @PathParam("propName") String propName)
      throws Exception {
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    final GetAliasPropertyResponse response =
        instantiateJerseyResponse(GetAliasPropertyResponse.class);
    final Aliases aliases = readAliasesFromZk();
    if (aliases != null) {
      String value = aliases.getCollectionAliasProperties(aliasName).get(propName);
      if (value != null) {
        response.value = value;
      } else {
        throw new SolrException(SolrException.ErrorCode.NOT_FOUND, propName + " not found");
      }
    }

    return response;
  }

  private Aliases readAliasesFromZk() throws Exception {
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    final ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
    // Make sure we have the latest alias info, since a user has explicitly invoked an alias API
    zkStateReader.getAliasesManager().update();
    return zkStateReader.getAliases();
  }

  @PUT
  @PermissionName(COLL_EDIT_PERM)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @Operation(
      summary = "Update properties for a collection alias.",
      tags = {"aliases"})
  public SolrJerseyResponse updateAliasProperties(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @RequestBody(description = "Properties that need to be updated", required = true)
          UpdateAliasPropertiesRequestBody requestBody)
      throws Exception {

    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }

    recordCollectionForLogAndTracing(null, solrQueryRequest);

    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    modifyAliasProperties(aliasName, requestBody.properties, requestBody.async);
    return response;
  }

  @PUT
  @Path("/{propName}")
  @PermissionName(COLL_EDIT_PERM)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @Operation(
      summary = "Update a specific property for a collection alias.",
      tags = {"aliases"})
  public SolrJerseyResponse createOrUpdateAliasProperty(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @Parameter(description = "Property Name") @PathParam("propName") String propName,
      @RequestBody(description = "Property value that needs to be updated", required = true)
          UpdateAliasPropertyRequestBody requestBody)
      throws Exception {
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }

    recordCollectionForLogAndTracing(null, solrQueryRequest);

    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    modifyAliasProperty(aliasName, propName, requestBody.value);
    return response;
  }

  @DELETE
  @Path("/{propName}")
  @PermissionName(COLL_EDIT_PERM)
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @Operation(
      summary = "Delete a specific property for a collection alias.",
      tags = {"aliases"})
  public SolrJerseyResponse deleteAliasProperty(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @Parameter(description = "Property Name") @PathParam("propName") String propName)
      throws Exception {
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    modifyAliasProperty(aliasName, propName, null);
    return response;
  }

  private void modifyAliasProperty(String alias, String proertyName, Object value)
      throws Exception {
    Map<String, Object> props = new HashMap<>();
    // value can be null
    props.put(proertyName, value);
    modifyAliasProperties(alias, props, null);
  }

  /**
   * @param alias alias
   */
  private void modifyAliasProperties(String alias, Map<String, Object> properties, String async)
      throws Exception {
    // Note: success/no-op in the event of no properties supplied is intentional. Keeps code
    // simple and one less case for api-callers to check for.
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    final ZkNodeProps remoteMessage = createRemoteMessage(alias, properties, async);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.ALIASPROP,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    disableResponseCaching();
  }

  private static final String PROPERTIES = "property";

  public ZkNodeProps createRemoteMessage(
      String alias, Map<String, Object> properties, String async) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.ALIASPROP.toLower());
    remoteMessage.put(NAME, alias);
    remoteMessage.put(PROPERTIES, properties);
    if (async != null) {
      remoteMessage.put(ASYNC, async);
    }
    return new ZkNodeProps(remoteMessage);
  }

  public static class UpdateAliasPropertiesRequestBody implements JacksonReflectMapWriter {

    @Schema(description = "Properties and values to be updated on alias.")
    @JsonProperty(value = "properties", required = true)
    public Map<String, Object> properties;

    @Schema(description = "Request ID to track this action which will be processed asynchronously.")
    @JsonProperty("async")
    public String async;
  }

  public static class UpdateAliasPropertyRequestBody implements JacksonReflectMapWriter {
    @JsonProperty(required = true)
    public Object value;
  }

  public static class GetAllAliasPropertiesResponse extends SolrJerseyResponse {
    @JsonProperty("properties")
    @Schema(description = "Properties and values associated with alias.")
    public Map<String, String> properties;
  }

  public static class GetAliasPropertyResponse extends SolrJerseyResponse {
    @JsonProperty("value")
    @Schema(description = "Property value.")
    public String value;
  }
}
