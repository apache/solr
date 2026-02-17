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
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.admin.AdminHandlersProxy;
import org.apache.solr.handler.admin.SystemInfoProvider;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of the V2 JerseyResource /node/info/system */
public class GetNodeSystemInfo extends JerseyResource implements NodeSystemInfoApi {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrQueryRequest solrQueryRequest;
  private final SolrQueryResponse solrQueryResponse;

  @Inject
  public GetNodeSystemInfo(SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.CONFIG_READ_PERM)
  public NodeSystemResponse getNodeSystemInfo(String nodes) {
    solrQueryResponse.setHttpCaching(false);

    // TODO: AdminHandlersProxy does not support V2: PRs #4057, #3991
    try {
      // TODO: Should use the "nodes" param
      if (solrQueryRequest.getCoreContainer() != null
          && AdminHandlersProxy.maybeProxyToNodes(
              solrQueryRequest, solrQueryResponse, solrQueryRequest.getCoreContainer())) {
        return null;
      }
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error occurred while proxying to other node", e);
    }

    SystemInfoProvider provider = new SystemInfoProvider(solrQueryRequest);
    NodeSystemResponse response = instantiateJerseyResponse(NodeSystemResponse.class);
    provider.getNodeSystemInfo(response);
    if (response.nodeInfo != null && log.isTraceEnabled()) {
      log.trace("Node {}, core root: {}", response.nodeInfo.node, response.nodeInfo.coreRoot);
    }
    return response;
  }

  @Override
  public NodeSystemResponse getSpecificNodeSystemInfo(String requestedInfo, String nodes) {
    solrQueryResponse.setHttpCaching(false);

    // TODO: AdminHandlersProxy does not support V2: PRs #4057, #3991
    try {
      // TODO: Should use the "nodes" param
      if (solrQueryRequest.getCoreContainer() != null
          && AdminHandlersProxy.maybeProxyToNodes(
              solrQueryRequest, solrQueryResponse, solrQueryRequest.getCoreContainer())) {
        return null;
      }
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error occurred while proxying to other node", e);
    }

    NodeSystemResponse response = instantiateJerseyResponse(NodeSystemResponse.class);
    response.nodeInfo = new NodeSystemResponse.NodeSystemInfo();
    SystemInfoProvider provider = new SystemInfoProvider(solrQueryRequest);
    switch (requestedInfo) {
      case "gpu":
        response.nodeInfo.gpu = provider.getGpuInfo();
        break;
      case "jvm":
        response.nodeInfo.jvm = provider.getJvmInfo();
        break;
      case "lucene":
        response.nodeInfo.lucene = provider.getLuceneInfo();
        break;
      case "security":
        response.nodeInfo.security = provider.getSecurityInfo();
        break;
      case "system":
        response.nodeInfo.system = provider.getSystemInfo();
        break;
      default:
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Unknown parameter: " + requestedInfo);
    }
    return response;
  }
}
