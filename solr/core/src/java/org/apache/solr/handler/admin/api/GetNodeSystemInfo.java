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
package org.apache.solr.handler.admin.api;

import jakarta.inject.Inject;
import java.lang.invoke.MethodHandles;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.NodeSystemInfoApi;
import org.apache.solr.client.api.model.NodeSystemResponse;
import org.apache.solr.client.solrj.request.SystemApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.SystemInfoProvider;
import org.apache.solr.handler.admin.proxy.V2SolrRequestBasedProxy;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for {@link NodeSystemInfoApi} */
public class GetNodeSystemInfo extends JerseyResource implements NodeSystemInfoApi {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final SolrQueryRequest solrQueryRequest;
  private final SolrQueryResponse solrQueryResponse;

  @Inject
  public GetNodeSystemInfo(SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
    this.coreContainer = solrQueryRequest.getCoreContainer();
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.CONFIG_READ_PERM)
  public NodeSystemResponse getNodeSystemInfo(String nodes) {
    NodeSystemResponse response = instantiateJerseyResponse(NodeSystemResponse.class);
    solrQueryResponse.setHttpCaching(false);

    if (proxyToNodes(response, nodes)) {
      return response; // Request handled via proxying
    }

    // No proxying done; populate data for this node.
    SystemInfoProvider provider = new SystemInfoProvider(solrQueryRequest);
    provider.getNodeSystemInfo(response);
    if (log.isTraceEnabled()) {
      log.trace("Node {}, core root: {}", response.node, response.coreRoot);
    }
    return response;
  }

  private boolean proxyToNodes(NodeSystemResponse response, String nodes) {
    try {
      if (coreContainer != null) {
        final var req = new SystemApi.GetNodeSystemInfo();
        req.setNodes(nodes);
        final var reqProxy =
            new V2SolrRequestBasedProxy<NodeSystemResponse>(coreContainer, req) {
              @Override
              public void processTypedProxiedResponse(
                  String nodeName, NodeSystemResponse proxiedResponse) {
                response.remoteNodeData.put(nodeName, proxiedResponse);
              }
            };
        return reqProxy.proxyRequest();
      }
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error occurred while proxying to other node", e);
    }
    return false;
  }
}
