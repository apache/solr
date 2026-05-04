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

import java.util.Objects;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * @since solr 1.3
 */
public class QueryRequest extends CollectionRequiringSolrRequest<QueryResponse> {

  private final SolrParams query;

  public QueryRequest() {
    super(METHOD.GET, "/select", SolrRequestType.QUERY);
    query = SolrParams.of();
  }

  public QueryRequest(SolrParams q) {
    super(METHOD.GET, pathFromParams(Objects.requireNonNull(q)), SolrRequestType.QUERY);
    query = paramsWithoutQt(q);
  }

  public QueryRequest(SolrParams q, METHOD method) {
    super(method, pathFromParams(Objects.requireNonNull(q)), SolrRequestType.QUERY);
    query = paramsWithoutQt(q);
  }

  public QueryRequest(String path, SolrParams q) {
    super(METHOD.GET, Objects.requireNonNull(path), SolrRequestType.QUERY);
    query = Objects.requireNonNull(q);
  }

  public QueryRequest(String path, SolrParams q, METHOD method) {
    super(method, Objects.requireNonNull(path), SolrRequestType.QUERY);
    query = Objects.requireNonNull(q);
  }

  private static String pathFromParams(SolrParams q) {
    String qt = q.get(CommonParams.QT);
    return (qt != null && qt.startsWith("/")) ? qt : "/select";
  }

  private static SolrParams paramsWithoutQt(SolrParams q) {
    if (q.get(CommonParams.QT) == null) return q;
    ModifiableSolrParams params = new ModifiableSolrParams(q);
    params.remove(CommonParams.QT);
    return params;
  }

  // ---------------------------------------------------------------------------------
  // ---------------------------------------------------------------------------------

  @Override
  protected QueryResponse createResponse(NamedList<Object> namedList) {
    return new QueryResponse();
  }

  @Override
  public SolrParams getParams() {
    return query;
  }
}
