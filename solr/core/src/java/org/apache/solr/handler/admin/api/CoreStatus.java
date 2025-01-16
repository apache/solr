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

import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import jakarta.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.endpoint.CoreApis;
import org.apache.solr.client.api.model.CoreStatusResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.CoreAdminOperation;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for getting the status of one or all cores.
 *
 * <p>This API (GET /v2/cores/coreName is analogous to the v1 /admin/cores?action=status command.
 */
public class CoreStatus extends CoreAdminAPIBase implements CoreApis.GetStatus {

  @Inject
  public CoreStatus(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public CoreStatusResponse getAllCoreStatus(Boolean indexInfo) throws IOException {
    return fetchStatusInfo(coreContainer, null, indexInfo);
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public CoreStatusResponse getCoreStatus(String coreName, Boolean indexInfo) throws IOException {
    return fetchStatusInfo(coreContainer, coreName, indexInfo);
  }

  public static CoreStatusResponse fetchStatusInfo(
      CoreContainer coreContainer, String coreName, Boolean indexInfo) throws IOException {
    final var response = new CoreStatusResponse();
    response.initFailures = new HashMap<>();

    for (Map.Entry<String, CoreContainer.CoreLoadFailure> failure :
        coreContainer.getCoreInitFailures().entrySet()) {

      // Skip irrelevant initFailures if we're only interested in a single core
      if (coreName != null && !failure.getKey().equals(coreName)) continue;

      response.initFailures.put(failure.getKey(), failure.getValue().exception);
    }

    // Populate status for each core
    final var coreNameList =
        (coreName != null) ? List.of(coreName) : coreContainer.getAllCoreNames();
    coreNameList.sort(null);
    response.status = new HashMap<>();
    for (String toPopulate : coreNameList) {
      // TODO This is where I left off at 1/15.  Next TODO items are:
      // 5. Look into how v1 code currently serializes Exceptions (are we just toString-ing them)
      // 6. Look into NOCOMMIT and other todo comments
      // 7. Look into the failing tests from create-core side
      response.status.put(
          toPopulate, CoreAdminOperation.getCoreStatus(coreContainer, toPopulate, indexInfo));
    }

    return response;
  }
}
