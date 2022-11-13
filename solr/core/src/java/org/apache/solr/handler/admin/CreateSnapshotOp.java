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

import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.api.CoreSnapshotAPI;
import org.apache.solr.handler.api.V2ApiUtils;

class CreateSnapshotOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();
    final String coreName = params.required().get(CoreAdminParams.CORE);
    final String commitName = params.required().get(CoreAdminParams.COMMIT_NAME);

    final CoreContainer coreContainer = it.handler.getCoreContainer();
    final CoreSnapshotAPI coreSnapshotAPI =
        new CoreSnapshotAPI(it.req, it.rsp, coreContainer, it.handler.getCoreAdminAsyncTracker());

    final CoreSnapshotAPI.CreateSnapshotResponse response =
        coreSnapshotAPI.createSnapshot(coreName, commitName, null);

    V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
  }
}
