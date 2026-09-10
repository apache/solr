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
import org.apache.solr.client.solrj.WrappedSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;

/**
 * Uses a v2 {@link SolrRequest} instance to proxy requests to other nodes.
 *
 * <p>While this implementation is intended for v2 requests, callers may use it for v1 requests as
 * well by overriding {@link #processProxiedResponse(String, NamedList)} to do something more
 * appropriate with the response.
 */
public abstract class V2SolrRequestBasedProxy<T> extends RemoteRequestProxy {

  private SolrRequest<T> solrRequest;

  public V2SolrRequestBasedProxy(CoreContainer coreContainer, SolrRequest<T> solrRequest) {
    super(coreContainer);
    this.solrRequest = solrRequest;
  }

  @Override
  public boolean shouldProxy() {
    String nodeNames = solrRequest.getParams().get(PARAM_NODES);
    if (nodeNames == null || nodeNames.isEmpty()) {
      return false; // No nodes parameter, handle locally
    }
    return true;
  }

  @Override
  public Collection<String> getDestinationNodes() {
    return validateNodeNames(solrRequest.getParams().get(PARAM_NODES));
  }

  @Override
  public SolrRequest<T> prepareProxiedRequest() {
    return new WrappedSolrRequest<>(solrRequest) {
      @Override
      public SolrParams getParams() {
        final var originalParams = this.wrapped.getParams();
        final var mutable =
            (originalParams instanceof ModifiableSolrParams myMut)
                ? myMut
                : new ModifiableSolrParams(originalParams);
        mutable.remove(PARAM_NODES);
        return mutable;
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processProxiedResponse(String nodeName, NamedList<Object> proxiedResponse) {
    final var typedResponse = (T) proxiedResponse.get("response");
    processTypedProxiedResponse(nodeName, typedResponse);
  }

  protected abstract void processTypedProxiedResponse(String nodeName, T proxiedResponse);
}
