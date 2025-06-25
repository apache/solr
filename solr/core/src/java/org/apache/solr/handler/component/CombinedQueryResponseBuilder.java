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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * The CombinedQueryResponseBuilder class extends the ResponseBuilder class and is responsible for
 * building a combined response for multiple SearchComponent objects. It orchestrates the process of
 * constructing the SolrQueryResponse by aggregating results from various components.
 */
public class CombinedQueryResponseBuilder extends ResponseBuilder {

  public final List<ResponseBuilder> responseBuilders = new ArrayList<>();

  /**
   * Constructs a CombinedQueryResponseBuilder instance.
   *
   * @param req the SolrQueryRequest object containing the query parameters and context.
   * @param rsp the SolrQueryResponse object to which the combined results will be added.
   * @param components a list of SearchComponent objects that will be used to build the response.
   */
  public CombinedQueryResponseBuilder(
      SolrQueryRequest req, SolrQueryResponse rsp, List<SearchComponent> components) {
    super(req, rsp, components);
  }
}
