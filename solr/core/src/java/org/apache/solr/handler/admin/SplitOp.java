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

import java.util.Arrays;
import java.util.Optional;
import org.apache.solr.client.api.model.SplitCoreRequestBody;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.api.SplitCoreAPI;
import org.apache.solr.handler.api.V2ApiUtils;

/**
 * CoreAdminOp implementation for shard splits. This request is enqueued when {@link
 * org.apache.solr.cloud.api.collections.SplitShardCmd} is processed. This operation handles two
 * types of requests: 1. If {@link CommonAdminParams#SPLIT_BY_PREFIX} is true, the request to
 * calculate document ranges for the sub-shards is processed here. 2. For any split request, the
 * actual index split is processed here.
 *
 * <p>The actual split logic lives in {@link SplitCoreAPI}.
 */
class SplitOp implements CoreAdminHandler.CoreAdminOp {

  @Override
  public boolean isExpensive() {
    return true;
  }

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();
    String cname = params.get(CoreAdminParams.CORE, "");

    final var requestBody = new SplitCoreRequestBody();
    Optional.ofNullable(params.getParams(CommonParams.PATH))
        .ifPresent(val -> requestBody.path = Arrays.asList(val));
    Optional.ofNullable(params.getParams(CoreAdminParams.TARGET_CORE))
        .ifPresent(val -> requestBody.targetCore = Arrays.asList(val));
    requestBody.splitKey = params.get(CommonAdminParams.SPLIT_KEY);
    requestBody.splitMethod = params.get(CommonAdminParams.SPLIT_METHOD);
    requestBody.getRanges = params.getBool(CoreAdminParams.GET_RANGES);
    requestBody.ranges = params.get(CoreAdminParams.RANGES);
    // 'async' is deliberately not copied here: CoreAdminHandler.handleRequestBody() already
    // wraps this whole execute() call in an async task using the v1 request's 'async' param, so
    // passing it through as well would make SplitCoreAPI try to register the same task id twice.

    final var splitCoreApi =
        new SplitCoreAPI(
            it.handler.coreContainer, it.handler.coreAdminAsyncTracker, it.req, it.rsp);
    final var response = splitCoreApi.splitCore(cname, requestBody);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(it.rsp, response);
  }
}
