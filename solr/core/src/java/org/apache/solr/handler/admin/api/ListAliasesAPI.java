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
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for listing aliases
 *
 * <p>This API <code>GET /api/aliases</code> is analogous to the v1 <code>GET /api/cluster/aliases
 * </code> API.
 */
@Path("/aliases")
public class ListAliasesAPI extends AdminAPIBase {

  @Inject
  public ListAliasesAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_READ_PERM)
  @Operation(
      summary = "List the existing collection aliases.",
      tags = {"aliases"})
  public GetAliasesResponse getAliases() throws Exception {
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    final GetAliasesResponse response = instantiateJerseyResponse(GetAliasesResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();

    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();

    // if someone calls listAliases, lets ensure we return an up to date response
    zkStateReader.getAliasesManager().update();

    Aliases aliases = zkStateReader.getAliases();

    if (aliases != null) {
      // the aliases themselves...
      response.aliases = aliases.getCollectionAliasMap();
      // Any properties for the above aliases.
      Map<String, Map<String, String>> meta = new LinkedHashMap<>();
      for (String alias : aliases.getCollectionAliasListMap().keySet()) {
        Map<String, String> collectionAliasProperties = aliases.getCollectionAliasProperties(alias);
        if (!collectionAliasProperties.isEmpty()) {
          meta.put(alias, collectionAliasProperties);
        }
      }
      response.properties = meta;
    }

    return response;
  }

  @GET
  @Path("/{aliasName}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_READ_PERM)
  @Operation(
      summary = "Get details for a specific collection alias.",
      tags = {"aliases"})
  public GetAliaseByNameResponse getAliasByName(
      @Parameter(description = "Alias name.", required = true) @PathParam("aliasName")
          String aliasName)
      throws Exception {
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    final GetAliaseByNameResponse response =
        instantiateJerseyResponse(GetAliaseByNameResponse.class);
    response.alias = aliasName;

    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();
    // if someone calls listAliases, lets ensure we return an up to date response
    zkStateReader.getAliasesManager().update();

    Aliases aliases = zkStateReader.getAliases();
    if (aliases != null) {
      response.properties = aliases.getCollectionAliasProperties(aliasName);
    }

    return response;
  }

  /** Response for {@link ListAliasesAPI}. */
  public static class GetAliasesResponse extends SolrJerseyResponse {
    @JsonProperty("aliases")
    public Map<String, String> aliases;

    @JsonProperty("properties")
    public Map<String, Map<String, String>> properties;
  }

  /** Response for {@link ListAliasesAPI}. */
  public static class GetAliaseByNameResponse extends SolrJerseyResponse {
    @JsonProperty("name")
    public String alias;

    @JsonProperty("properties")
    public Map<String, String> properties;
  }
}
