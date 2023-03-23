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

package org.apache.solr.handler.admin;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;

import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.api.InstallCoreDataAPI;
import org.apache.solr.handler.api.V2ApiUtils;

/**
 * v1 shim implementation of the "Install Core Data" API, a core-admin API used to implement the
 * "Install Shard Data" Collection-Admin functionality
 *
 * <p>Converts v1-style query parameters into a v2-style request body and delegating to {@link
 * InstallCoreDataAPI}.
 */
public class InstallCoreDataOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();
    final String coreName = params.required().get(CoreAdminParams.CORE);

    final InstallCoreDataAPI api =
        new InstallCoreDataAPI(
            it.handler.getCoreContainer(), it.handler.getCoreAdminAsyncTracker(), it.req, it.rsp);
    final InstallCoreDataAPI.InstallCoreDataRequestBody requestBody =
        new InstallCoreDataAPI.InstallCoreDataRequestBody();
    requestBody.repository = params.get(BACKUP_REPOSITORY);
    requestBody.location = params.get(BACKUP_LOCATION);
    requestBody.asyncId = params.get(ASYNC);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        it.rsp, api.installCoreData(coreName, requestBody));
  }
}
