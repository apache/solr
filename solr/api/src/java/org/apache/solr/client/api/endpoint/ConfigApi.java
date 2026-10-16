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
package org.apache.solr.client.api.endpoint;

import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.ConfigInfoResponse;
import org.apache.solr.client.api.model.ConfigOverlayResponse;
import org.apache.solr.client.api.model.ConfigZnodeVersionResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

/** V2 API for reading and modifying a core/collection's config */
public interface ConfigApi {

  @Path(INDEX_PATH_PREFIX + "/config")
  interface Get {
    @GET
    @StoreApiParameters
    @Operation(
        summary = "Fetch the entire config of the specified core or collection",
        tags = {"config"})
    ConfigInfoResponse getConfig(
        @QueryParam("expandParams")
            @DefaultValue("false")
            @Parameter(description = "Expand request handler useParams and effective parameters")
            boolean expandParams);

    @GET
    @Path("/overlay")
    @StoreApiParameters
    @Operation(
        summary = "Fetch the config overlay of the specified core or collection",
        tags = {"config"})
    ConfigOverlayResponse getOverlay();

    @GET
    @Path("/znodeVersion")
    @StoreApiParameters
    @Operation(
        summary = "Fetch znode versions for config overlay and request params",
        tags = {"config"})
    ConfigZnodeVersionResponse getZnodeVersion(
        @QueryParam("overlay")
            @DefaultValue("-1")
            @Parameter(description = "Expected overlay znode version")
            int expectedOverlayVersion,
        @QueryParam("params")
            @DefaultValue("-1")
            @Parameter(description = "Expected request-params znode version")
            int expectedParamsVersion);

    // TODO: Keep this regex in sync with GetConfig.MIGRATED_CONFIG_COMPONENTS ("searchComponents"
    // and "queryResponseWriters" here are the external, pluralized names for the internal
    // "searchComponent"/"queryResponseWriter" sections; see GetConfig#toExternalComponentName)
    @GET
    @Path(
        "/{component:updateHandler|directoryFactory|indexConfig|codecFactory|query|searchComponents|queryResponseWriters|luceneMatchVersion|initParams|listeners|requestDispatcher|jmx|queryParser|valueSourceParser|transformer|updateProcessor|updateRequestProcessorChain|queryConverter|cache|circuitBreaker|expressible|schemaFactory|restManager|statsCache|recoveryStrategy|indexReaderFactory}")
    @StoreApiParameters
    @Operation(
        summary = "Fetch a config section",
        tags = {"config"})
    ConfigInfoResponse getConfigComponent(
        @PathParam("component") @Parameter(description = "Name of the config component")
            String component);

    // TODO: Keep this regex in sync with GetConfig.MIGRATED_CONFIG_COMPONENTS ("searchComponents"
    // and "queryResponseWriters" here are the external, pluralized names for the internal
    // "searchComponent"/"queryResponseWriter" sections; see GetConfig#toExternalComponentName)
    @GET
    @Path(
        "/{component:updateHandler|directoryFactory|indexConfig|codecFactory|query|searchComponents|queryResponseWriters|luceneMatchVersion|initParams|listeners|requestDispatcher|jmx|queryParser|valueSourceParser|transformer|updateProcessor|updateRequestProcessorChain|queryConverter|cache|circuitBreaker|expressible|schemaFactory|restManager|statsCache|recoveryStrategy|indexReaderFactory}/{componentName}")
    @StoreApiParameters
    @Operation(
        summary = "Fetch a single named entry within a config section",
        tags = {"config"})
    ConfigInfoResponse getConfigComponentByName(
        @PathParam("component") @Parameter(description = "Name of the config component")
            String component,
        @PathParam("componentName") @Parameter(description = "Name of the entry within the section")
            String componentName,
        @QueryParam("meta")
            @DefaultValue("false")
            @Parameter(description = "Include package metadata (_packageinfo_)")
            boolean meta);
  }
}
