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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.apache.solr.client.api.model.MigrateReplicasRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** V2 API definition for migrating replicas from a set of nodes to another set of nodes. */
@Path("cluster/replicas/migrate")
public interface MigrateReplicasApi {
  @POST
  @Operation(
      summary = "Migrate Replicas from a given set of nodes.",
      tags = {"cluster"})
  SolrJerseyResponse migrateReplicas(
      @RequestBody(description = "Contains user provided parameters", required = true)
          MigrateReplicasRequestBody requestBody)
      throws Exception;
}
