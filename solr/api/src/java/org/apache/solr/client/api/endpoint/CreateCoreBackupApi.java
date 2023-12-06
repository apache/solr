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
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.CreateCoreBackupRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

@Path("/cores/{coreName}/backups")
public interface CreateCoreBackupApi {

  @POST
  @Operation(
      summary = "Creates a core-level backup",
      tags = {"core-backups"})
  SolrJerseyResponse createBackup(
      @Parameter(description = "The name of the core.") @PathParam("coreName") String coreName,
      @Schema(description = "Additional backup params")
          CreateCoreBackupRequestBody backupCoreRequestBody)
      throws Exception;
}
