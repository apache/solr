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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.api.model.CoreStatusResponse;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;

class StatusOp implements CoreAdminHandler.CoreAdminOp {

  public static CoreStatusResponse fetchStatusInfo(CoreContainer coreContainer, String coreName, Boolean indexInfo) throws IOException {
    final var response = new CoreStatusResponse();
    response.initFailures = new HashMap<>();

    for (Map.Entry<String, CoreContainer.CoreLoadFailure> failure : coreContainer.getCoreInitFailures().entrySet()) {

      // Skip irrelevant initFailures if we're only interested in a single core
      if (coreName != null && !failure.getKey().equals(coreName))
        continue;

      response.initFailures.put(failure.getKey(), failure.getValue().exception);
    }

    // Populate status for each core
    final var coreNameList = (coreName != null) ? List.of(coreName) : coreContainer.getAllCoreNames();
    coreNameList.sort(null);
    response.status = new HashMap<>();
    for (String toPopulate : coreNameList) {
      // TODO This is where I left off at 1/15.  Next TODO items are:
      // 1. port CoreAdminOperation.getCoreStatus over to return a POJO
      // 2. modify execute() below to call this method (fetchStatusInfo), and then squash the strong-type into a NL
      // 3. Move this method (fetchStatusInfo) into the v2 API class, and use it to implement both single and all-core fetching
      // 4. Trace through the v1 and v2 codepaths to make sure registration, plumbing, etc. are all in place
      // 5. Look into how v1 code currently serializes Exceptions (are we just toString-ing them)
      // 6. Look into NOCOMMIT and other todo comments
      // 7. Look into the failing tests from create-core side
      response.status.put(toPopulate, CoreAdminOperation.getCoreStatus(coreContainer, toPopulate, indexInfo));
    }

    return response;
  }

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();

    String cname = params.get(CoreAdminParams.CORE);
    String indexInfo = params.get(CoreAdminParams.INDEX_INFO);
    boolean isIndexInfoNeeded = Boolean.parseBoolean(null == indexInfo ? "true" : indexInfo);
    NamedList<Object> status = new SimpleOrderedMap<>();

    if (cname == null) {
      List<String> nameList = it.handler.coreContainer.getAllCoreNames();
      nameList.sort(null);
      for (String name : nameList) {
        status.add(
            name,
            CoreAdminOperation.getCoreStatus(it.handler.coreContainer, name, isIndexInfoNeeded));
      }
    } else {
      status.add(
          cname,
          CoreAdminOperation.getCoreStatus(it.handler.coreContainer, cname, isIndexInfoNeeded));
    }
    it.rsp.add("status", status);
  }
}
