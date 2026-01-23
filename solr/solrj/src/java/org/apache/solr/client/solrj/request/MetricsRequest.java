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
import org.apache.solr.client.solrj.response.InputStreamResponse;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/** Request to V1 "/admin/metrics" or V2 "/metrics" */
public class MetricsRequest extends SolrRequest<InputStreamResponse> {

  private static final long serialVersionUID = 1L;

  private final SolrParams params;

  /** Request to "/admin/metrics" by default, without params */
  public MetricsRequest() {
    this(new ModifiableSolrParams());
  }

  /**
   * @param path the HTTP path to use for this request. Supports V1 "/admin/metrics" (default) or V2
   *     "/metrics"
   */
  public MetricsRequest(String path) {
    this(path, new ModifiableSolrParams());
  }

  /**
   * @param params the Solr parameters to use for this request.
   */
  public MetricsRequest(SolrParams params) {
    this(CommonParams.METRICS_PATH, params);
  }

  /**
   * @param params the Solr parameters to use for this request.
   */
  public MetricsRequest(String path, SolrParams params) {
    super(METHOD.GET, path, SolrRequestType.ADMIN);
    if (!path.endsWith("/metrics")) {
      throw new SolrException(
          SolrException.ErrorCode.INVALID_STATE, "Request path not supported: " + path);
    }
    this.params = params;
    // Set response parser according to "wt".
    if ("openmetrics".equals(params.get(CommonParams.WT))) {
      setResponseParser(new InputStreamResponseParser("openmetrics"));
    } else {
      setResponseParser(new InputStreamResponseParser("prometheus"));
    }
  }

  @Override
  public SolrParams getParams() {
    return params;
  }

  @Override
  protected InputStreamResponse createResponse(NamedList<Object> namedList) {
    return new InputStreamResponse();
  }

  @Override
  public ApiVersion getApiVersion() {
    if (CommonParams.METRICS_PATH.equals(getPath())) {
      // (/solr) /admin/metrics
      return ApiVersion.V1;
    }
    // Ref. org.apache.solr.client.api.endpoint.MetricsApi : /metrics
    return ApiVersion.V2;
  }
}
