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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.invoke.MethodHandles;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/{a:cores|collections}/{collectionName}/schema")
public class GetSchemaZkVersionAPI extends JerseyResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private SolrCore solrCore;

  @Inject
  public GetSchemaZkVersionAPI(SolrCore solrCore) {
    this.solrCore = solrCore;
  }

  @GET
  @Path("/zkversion")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_ATOM_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.SCHEMA_READ_PERM)
  public SchemaZkVersionResponse getSchemaZkVersion(
      @DefaultValue("-1") @QueryParam("refreshIfBelowVersion") Integer refreshIfBelowVersion)
      throws Exception {
    final SchemaZkVersionResponse response =
        instantiateJerseyResponse(SchemaZkVersionResponse.class);
    int zkVersion = -1;
    if (solrCore.getLatestSchema() instanceof ManagedIndexSchema) {
      ManagedIndexSchema managed = (ManagedIndexSchema) solrCore.getLatestSchema();
      zkVersion = managed.getSchemaZkVersion();
      if (refreshIfBelowVersion != -1 && zkVersion < refreshIfBelowVersion) {
        log.info(
            "REFRESHING SCHEMA (refreshIfBelowVersion={}, currentVersion={}) before returning version!",
            refreshIfBelowVersion,
            zkVersion);
        ZkSolrResourceLoader zkSolrResourceLoader =
            (ZkSolrResourceLoader) solrCore.getResourceLoader();
        ZkIndexSchemaReader zkIndexSchemaReader = zkSolrResourceLoader.getZkIndexSchemaReader();
        managed = zkIndexSchemaReader.refreshSchemaFromZk(refreshIfBelowVersion);
        zkVersion = managed.getSchemaZkVersion();
      }
    }
    response.zkversion = zkVersion;
    return response;
  }

  public static class SchemaZkVersionResponse extends SolrJerseyResponse {
    @JsonProperty("zkversion")
    public int zkversion;
  }
}
