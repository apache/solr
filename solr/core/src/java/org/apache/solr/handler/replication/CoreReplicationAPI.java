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
package org.apache.solr.handler.replication;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.security.PermissionNameProvider.Name.*;

import javax.inject.Inject;
import javax.ws.rs.Path;
import javax.ws.rs.GET;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.io.IOException;

@Path("/core/{coreName}/replication")
public class CoreReplicationAPI extends ReplicationAPIBase {

    @Inject
    public CoreReplicationAPI(
            CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
        super(coreContainer, req, rsp);
    }

    @GET
    @Path("/indexversion")
    @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
    @PermissionName(CORE_READ_PERM)
    public GetIndexResponse IndexVersionResponse(@Parameter(
            description = "The name of the core for which to retrieve the index version",
            required = true) @PathParam("coreName") String coreName) throws IOException {

        return fetchIndexVersion(coreName);
    }

    /** Response for {@link CoreReplicationAPI}. */
    public static class GetIndexResponse extends SolrJerseyResponse {

        @JsonProperty("indexversion")
        public Long indexVersion;

        @JsonProperty("generation")
        public Long generation;

        @JsonProperty("status")
        public String status;

    }

}
