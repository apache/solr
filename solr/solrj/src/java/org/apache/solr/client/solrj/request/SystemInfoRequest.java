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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/** Class to get a system info response. */
public class SystemInfoRequest extends SolrRequest<SystemInfoResponse> {

  private static final long serialVersionUID = 1L;

  private final SolrParams params;

  /** Request to "/admin/info/system" by default, without params. */
  public SystemInfoRequest() {
    // TODO: support V2 by default.  Requires refactoring throughout the CLI tools, at least
    this(CommonParams.SYSTEM_INFO_PATH);
  }

  /**
   * @param path the HTTP path to use for this request. Supports V1 "/admin/info/system" (default)
   *     or V2 "/node/system"
   */
  public SystemInfoRequest(String path) {
    this(path, new ModifiableSolrParams());
  }

  /**
   * @param params the Solr parameters to use for this request.
   */
  public SystemInfoRequest(SolrParams params) {
    this(CommonParams.SYSTEM_INFO_PATH, params);
  }

  /**
   * @param path the HTTP path to use for this request. Supports V1 "/admin/info/system" (default)
   *     or V2 "/node/system"
   * @param params query parameter names and values for making this request.
   */
  public SystemInfoRequest(String path, SolrParams params) {
    super(METHOD.GET, path, SolrRequestType.ADMIN);
    this.params = params;
  }

  @Override
  public SolrParams getParams() {
    return params;
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
