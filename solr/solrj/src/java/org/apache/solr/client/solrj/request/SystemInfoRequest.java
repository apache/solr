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
package org.apache.solr.client.solrj.request;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.SystemInfoResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/** Class to get a system info response. */
public class SystemInfoRequest extends SolrRequest<SystemInfoResponse> {

  private static final long serialVersionUID = 1L;

  public SystemInfoRequest() {
    this(CommonParams.SYSTEM_INFO_PATH);
  }

  public SystemInfoRequest(String path) {
    super(METHOD.GET, path, SolrRequestType.ADMIN);
  }

  @Override
  public SolrParams getParams() {
    return SolrParams.of(); // no params to return, but avoid NPE
  }

  @Override
  protected SystemInfoResponse createResponse(NamedList<Object> namedList) {
    return new SystemInfoResponse(namedList);
  }

  @Override
  public ApiVersion getApiVersion() {
    if (CommonParams.SYSTEM_INFO_PATH.equals(getPath())) {
      // (/solr) /admin/info/system
      return ApiVersion.V1;
    }
    // Ref. org.apache.solr.handler.admin.api.NodeSystemInfoAPI : /node/system
    return ApiVersion.V2;
  }
}
