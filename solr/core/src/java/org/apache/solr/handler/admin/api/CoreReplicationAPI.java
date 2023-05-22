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
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for inspecting and replicating indices
 *
 * <p>These APIs are analogous to the v1 /coreName/replication APIs.
 */
@Path("/cores/{coreName}/replication")
public class CoreReplicationAPI extends ReplicationAPIBase {

  @Inject
  public CoreReplicationAPI(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(solrCore, req, rsp);
  }

  @GET
  @Path("/indexversion")
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_READ_PERM)
  public IndexVersionResponse fetchIndexVersion() throws IOException {
    return doFetchIndexVersion();
  }

  /** Response for {@link CoreReplicationAPI#fetchIndexVersion()}. */
  public static class IndexVersionResponse extends SolrJerseyResponse {

    @JsonProperty("indexversion")
    public Long indexVersion;

    @JsonProperty("generation")
    public Long generation;

    @JsonProperty("status")
    public String status;

    public IndexVersionResponse() {}

    public IndexVersionResponse(Long indexVersion, Long generation, String status) {
      this.indexVersion = indexVersion;
      this.generation = generation;
      this.status = status;
    }
  }
}
