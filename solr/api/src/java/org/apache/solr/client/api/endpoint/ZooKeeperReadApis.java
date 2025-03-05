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

import static org.apache.solr.client.api.util.Constants.RAW_OUTPUT_PROPERTY;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;
import org.apache.solr.client.api.model.ZooKeeperListChildrenResponse;

/** V2 API definitions for Solr's ZooKeeper ready-proxy endpoint */
@Path("/cluster/zookeeper/")
public interface ZooKeeperReadApis {

  @GET
  @Path("/data{zkPath:.+}")
  @Operation(
      summary = "Return the data stored in a specified ZooKeeper node",
      tags = {"zookeeper-read"},
      extensions = {
        @Extension(properties = {@ExtensionProperty(name = RAW_OUTPUT_PROPERTY, value = "true")})
      })
  @Produces({"application/vnd.apache.solr.raw", MediaType.APPLICATION_JSON})
  StreamingOutput readNode(
      @Parameter(description = "The path of the node to read from ZooKeeper") @PathParam("zkPath")
          String zkPath);

  // The 'Operation' annotation is omitted intentionally here to ensure this API isn't picked up in
  // the OpenAPI spec and consequent code-generation.  The server side needs this method to be
  // different from 'readNode' above for security reasons (more privileges are needed to access
  // security.json), but it's the same logical API expressed by the 'readNode' signature above.
  @GET
  @Path("/data/security.json")
  @Produces({"application/vnd.apache.solr.raw", MediaType.APPLICATION_JSON})
  StreamingOutput readSecurityJsonNode();

  @GET
  @Path("/children{zkPath:.*}")
  @Produces({"application/json", "application/javabin"})
  @Operation(
      summary = "List and stat all children of a specified ZooKeeper node",
      tags = {"zookeeper-read"})
  ZooKeeperListChildrenResponse listNodes(
      @Parameter(description = "The path of the ZooKeeper node to stat and list children of")
          @PathParam("zkPath")
          String zkPath,
      @Parameter(
              description =
                  "Controls whether stat information for child nodes is included in the response. 'true' by default.")
          @QueryParam("children")
          Boolean includeChildren)
      throws Exception;
}
