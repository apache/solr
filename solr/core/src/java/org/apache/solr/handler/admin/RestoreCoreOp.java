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

import org.apache.solr.client.api.model.RestoreCoreRequestBody;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.api.RestoreCore;
import org.apache.solr.handler.api.V2ApiUtils;

class RestoreCoreOp implements CoreAdminHandler.CoreAdminOp {

  @Override
  public boolean isExpensive() {
    return true;
  }

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    final var requestBody = new RestoreCoreRequestBody();
    // "async" param intentionally omitted because CoreAdminHandler has already processed
    requestBody.name = params.get(CoreAdminParams.NAME);
    requestBody.shardBackupId = params.get(CoreAdminParams.SHARD_BACKUP_ID);
    requestBody.location = params.get(CoreAdminParams.BACKUP_LOCATION);
    requestBody.backupRepository = params.get(CoreAdminParams.BACKUP_REPOSITORY);
    RestoreCore.validateRequestBody(requestBody);

    final CoreContainer coreContainer = it.handler.getCoreContainer();
    final var api =
        new RestoreCore(coreContainer, it.req, it.rsp, it.handler.getCoreAdminAsyncTracker());
    final var response = api.restoreCore(cname, requestBody);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
  }

  public static RestoreCoreRequestBody createRequestFromV1Params(SolrParams params) {
    final var requestBody = new RestoreCoreRequestBody();
    // "async" param intentionally omitted because CoreAdminHandler has already processed
    requestBody.name = params.get(CoreAdminParams.NAME);
    requestBody.shardBackupId = params.get(CoreAdminParams.SHARD_BACKUP_ID);
    requestBody.location = params.get(CoreAdminParams.BACKUP_LOCATION);
    requestBody.backupRepository = params.get(CoreAdminParams.BACKUP_REPOSITORY);

    return requestBody;
  }
}
