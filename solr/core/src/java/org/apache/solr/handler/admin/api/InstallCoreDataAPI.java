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
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.RestoreCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * v2 implementation of the "Install Core Data" Core-Admin API
 *
 * <p>This is an internal API intended for use only by the Collection Admin "Install Shard Data"
 * API.
 */
@Path("/cores/{coreName}/install")
public class InstallCoreDataAPI extends CoreAdminAPIBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public InstallCoreDataAPI(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_EDIT_PERM)
  public SolrJerseyResponse installCoreData(
      @PathParam("coreName") String coreName, InstallCoreDataRequestBody requestBody)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

    if (requestBody == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Required request body is missing");
    }

    final ZkController zkController = coreContainer.getZkController();
    if (zkController == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'Install Core Data' API only supported in SolrCloud clusters");
    }

    try (BackupRepository repository = coreContainer.newBackupRepository(requestBody.repository);
        SolrCore core = coreContainer.getCore(coreName)) {
      String location = repository.getBackupLocation(requestBody.location);
      if (location == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "'location' is not specified as a" + " parameter or as a default repository property");
      }

      final URI locationUri = repository.createDirectoryURI(location);
      final CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
      if (!core.readOnly) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Failed to install data to core core="
                + core.getName()
                + "; collection must be in read-only mode prior to installing data to a core");
      }

      final RestoreCore restoreCore = RestoreCore.create(repository, core, locationUri, "");
      boolean success = restoreCore.doRestore();
      if (!success) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Failed to install data to core=" + core.getName());
      }

      // other replicas to-be-created will know that they are out of date by
      // looking at their term : 0 compare to term of this core : 1
      zkController
          .getShardTerms(cd.getCollectionName(), cd.getShardId())
          .ensureHighestTermsAreNotZero();
    }

    return response;
  }

  public static class InstallCoreDataRequestBody implements JacksonReflectMapWriter {
    // Expected to point to an index directory (e.g. data/techproducts_shard1_replica_n1/data/index)
    // for a single core that has previously been uploaded to the backup repository previously
    // uploaded to the backup repository.
    @JsonProperty("location")
    public String location;

    @JsonProperty("repository")
    public String repository;

    @JsonProperty("async")
    public String asyncId;
  }
}
