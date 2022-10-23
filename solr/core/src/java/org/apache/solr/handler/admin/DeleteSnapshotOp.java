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
import org.apache.solr.handler.admin.api.SnapshotAPI;

class DeleteSnapshotOp implements CoreAdminHandler.CoreAdminOp {

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final CoreContainer coreContainer = it.handler.getCoreContainer();

    final SnapshotAPI snapshotAPI = new SnapshotAPI(coreContainer);

    final SolrParams params = it.req.getParams();
    final String commitName = params.required().get(CoreAdminParams.COMMIT_NAME);
    final String coreName = params.required().get(CoreAdminParams.CORE);

    final SnapshotAPI.DeleteSnapshotResponse response = snapshotAPI.deleteSnapshot(coreName, commitName);


    it.rsp.add(CoreAdminParams.CORE, response.coreName);
    it.rsp.add(CoreAdminParams.COMMIT_NAME, response.commitName);
  }
}
