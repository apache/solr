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

import io.swagger.v3.oas.annotations.Parameter;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.DeleteCollectionSnapshotResponse;

public interface DeleteCollectionSnapshotApi {

  /** This API is analogous to V1's (POST /solr/admin/collections?action=DELETESNAPSHOT) */
  @DELETE
  @Path("/collections/{collName}/snapshots/{snapshotName}")
  DeleteCollectionSnapshotResponse deleteSnapshot(
      @Parameter(description = "The name of the collection.", required = true)
          @PathParam("collName")
          String collName,
      @Parameter(description = "The name of the snapshot to be deleted.", required = true)
          @PathParam("snapshotName")
          String snapshotName,
      @Parameter(description = "A flag that treats the collName parameter as a collection alias.")
          @DefaultValue("false")
          @QueryParam("followAliases")
          boolean followAliases,
      @QueryParam("async") String asyncId)
      throws Exception;
}
