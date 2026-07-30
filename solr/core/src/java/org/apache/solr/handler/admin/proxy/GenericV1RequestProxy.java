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
package org.apache.solr.handler.admin.proxy;

import java.util.Collection;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** Uses a v1 {@link GenericSolrRequest} to proxy the executing (v1) request to remote nodes */
public class GenericV1RequestProxy extends RemoteRequestProxy {

  private final ModifiableSolrParams params;
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;

  public GenericV1RequestProxy(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer);
    this.req = req;
    this.params = new ModifiableSolrParams(req.getParams());
    this.rsp = rsp;
  }

  @Override
  public boolean shouldProxy() {
    String nodeNames = params.get(getDestinationNodeParamName());
    if (nodeNames == null || nodeNames.isEmpty()) {
      return false; // No nodes parameter, handle locally
    }
    return true;
  }

  @Override
  public Collection<String> getDestinationNodes() {
    return validateNodeNames(params.get(getDestinationNodeParamName()));
  }

  @Override
  public SolrRequest<?> prepareProxiedRequest() {
    params.remove(getDestinationNodeParamName());
    return createGenericRequest(req.getPath(), params);
  }

  @Override
  public void processProxiedResponse(String nodeName, NamedList<Object> proxiedResponse) {
    rsp.add(nodeName, proxiedResponse);
  }

  /** The name of the query-param that indicates which node(s) should be proxied to. */
  protected String getDestinationNodeParamName() {
    return PARAM_NODES;
  }

  protected SolrRequest<?> createGenericRequest(String apiPath, SolrParams params) {
    return new GenericSolrRequest(SolrRequest.METHOD.GET, apiPath, params);
  }
}
