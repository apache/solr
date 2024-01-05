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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Optional;
import org.apache.solr.client.api.model.MergeIndexesRequestBody;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.handler.admin.api.MergeIndexes;
import org.apache.solr.handler.api.V2ApiUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeIndexesOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    final var requestBody = new MergeIndexesRequestBody();
    Optional.ofNullable(params.getParams(CoreAdminParams.INDEX_DIR))
        .ifPresent(val -> requestBody.indexDirs = Arrays.asList(val));
    Optional.ofNullable(params.getParams(CoreAdminParams.SRC_CORE))
        .ifPresent(val -> requestBody.srcCores = Arrays.asList(val));
    requestBody.updateChain = params.get(UpdateParams.UPDATE_CHAIN);
    final var mergeIndexesApi =
        new MergeIndexes(
            it.handler.coreContainer, it.handler.coreAdminAsyncTracker, it.req, it.rsp);
    final var response = mergeIndexesApi.mergeIndexes(cname, requestBody);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
  }
}
