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

import org.apache.solr.common.params.SolrParams;

/** A {@link GenericSolrRequest} implementation intended for v2 APIs */
public class GenericV2SolrRequest extends GenericSolrRequest {

  /**
   * @param m the HTTP method to use for this request
   * @param path the HTTP path to use for this request. If users are making a collection-aware
   *     request (i.e. {@link #setRequiresCollection(boolean)} is called with 'true'), only the
   *     section of the API path following the collection or core should be provided here.
   */
  public GenericV2SolrRequest(METHOD m, String path) {
    super(m, path);
  }

  /**
   * @param m the HTTP method to use for this request
   * @param path the HTTP path to use for this request. If users are making a collection-aware
   *     request (i.e. {@link #setRequiresCollection(boolean)} is called with 'true'), only the
   *     section of the API path following the collection or core should be provided here.
   * @param params query parameter names and values for making this request.
   */
  public GenericV2SolrRequest(METHOD m, String path, SolrParams params) {
    super(m, path);
    this.params = params;
  }

  public ApiVersion getApiVersion() {
    return ApiVersion.V2;
  }
}
