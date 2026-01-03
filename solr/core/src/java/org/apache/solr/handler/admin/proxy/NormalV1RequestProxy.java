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
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

// TODO Fix this name
public class NormalV1RequestProxy extends AdminHandlersProxy {

  private final ModifiableSolrParams params;
  private final SolrQueryResponse rsp;

  public NormalV1RequestProxy(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req);
    this.params = new ModifiableSolrParams(req.getParams());
    this.rsp = rsp;
  }

  @Override
  public boolean shouldProxy() {
    String nodeNames = params.get(PARAM_NODES);
    if (nodeNames == null || nodeNames.isEmpty()) {
      return false; // No nodes parameter, handle locally
    }
    return true;
  }

  @Override
  public Collection<String> getDestinationNodes() {
    return AdminHandlersProxy.resolveNodes(params.get(PARAM_NODES), coreContainer);
  }

  @Override
  public SolrRequest<?> prepareProxiedRequest() {
    params.remove(PARAM_NODES);
    return AdminHandlersProxy.createGenericRequest(req.getPath(), params);
  }

  @Override
  public void processProxiedResponse(String nodeName, NamedList<Object> proxiedResponse) {
    rsp.add(nodeName, proxiedResponse);
  }
}
