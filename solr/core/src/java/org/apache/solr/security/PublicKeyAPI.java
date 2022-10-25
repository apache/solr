/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.security;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;

/**
 * V2 API for fetching the public key of the receiving node.
 *
 * <p>This API is analogous to the v1 /admin/info/key endpoint.
 */
@Path("/node/key")
public class PublicKeyAPI extends JerseyResource {

  private final SolrNodeKeyPair nodeKeyPair;

  @Inject
  public PublicKeyAPI(SolrNodeKeyPair nodeKeyPair) {
    this.nodeKeyPair = nodeKeyPair;
  }

  @GET
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(PermissionNameProvider.Name.ALL)
  public PublicKeyResponse getPublicKey() {
    final PublicKeyResponse response = instantiateJerseyResponse(PublicKeyResponse.class);
    response.key = nodeKeyPair.getKeyPair().getPublicKeyStr();
    return response;
  }

  public static class PublicKeyResponse extends SolrJerseyResponse {
    @JsonProperty("key")
    @Schema(description = "The public key of the receiving Solr node.")
    public String key;
  }
}
