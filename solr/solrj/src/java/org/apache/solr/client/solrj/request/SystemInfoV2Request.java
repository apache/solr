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

import org.apache.solr.client.api.util.Constants;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.SystemInfoV2Response;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/** Class to get a system info response. */
public class SystemInfoV2Request extends SolrRequest<SystemInfoV2Response> {

  private static final long serialVersionUID = 1L;

  private final SolrParams params;

  /** Request to "/node/info/system" by default, without params. */
  public SystemInfoV2Request() {
    // TODO: support V2 by default.  Requires refactoring throughout the CLI tools, at least
    this(Constants.NODE_INFO_SYSTEM_PATH, new ModifiableSolrParams());
  }

  /**
   * @param params the Solr parameters to use for this request.
   */
  public SystemInfoV2Request(SolrParams params) {
    this(Constants.NODE_INFO_SYSTEM_PATH, params);
  }

  /**
   * @param path the HTTP path to use for this request. Supports V2 "/node/info/system"
   * @param params query parameter names and values for making this request.
   */
  public SystemInfoV2Request(String path, SolrParams params) {
    super(METHOD.GET, path, SolrRequestType.ADMIN);
    if (!path.equals(Constants.NODE_INFO_SYSTEM_PATH)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Unsupported request path: " + path);
    }
    this.params = params;
  }

  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  protected SystemInfoV2Response createResponse(NamedList<Object> namedList) {
    return new SystemInfoV2Response(namedList);
  }

  @Override
  public ApiVersion getApiVersion() {
    return ApiVersion.V2;
  }
}
