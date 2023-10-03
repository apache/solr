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

import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import java.net.URI;
import org.apache.solr.client.api.endpoint.InstallCoreDataApi;
import org.apache.solr.client.api.model.InstallCoreDataRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.RestoreCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementation of the "Install Core Data" Core-Admin API
 *
 * <p>This is an internal API intended for use only by the Collection Admin "Install Shard Data"
 * API.
 */
public class InstallCoreData extends CoreAdminAPIBase implements InstallCoreDataApi {

  public InstallCoreData(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
  }

  @Override
  @PermissionName(CORE_EDIT_PERM)
  public SolrJerseyResponse installCoreData(String coreName, InstallCoreDataRequestBody requestBody)
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
}
