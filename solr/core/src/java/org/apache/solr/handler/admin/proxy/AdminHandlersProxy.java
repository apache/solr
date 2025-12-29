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

import static org.apache.solr.common.params.CommonParams.WT;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static methods to proxy calls to an Admin (GET) API to other nodes in the cluster and return a
 * combined response
 */
public abstract class AdminHandlersProxy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static final String PARAM_NODES = "nodes";
  // TODO Move to NormalV1RequestProxy if not used elsewhere when finished
  protected static final String PARAM_NODE = "node";
  // TODO Move to PrometheusRequestProxy if not used elsewhere when finished
  private static final long PROMETHEUS_FETCH_TIMEOUT_SECONDS = 10;

  protected final CoreContainer
      coreContainer; // TODO reduce this to just ZkStateReader or something similar
  protected final SolrQueryRequest req;

  public AdminHandlersProxy(CoreContainer coreContainer, SolrQueryRequest req) {
    this.coreContainer = coreContainer;
    this.req = req;
  }

  public abstract boolean shouldProxy();

  /**
   * TODO fill in these javadocs
   *
   * <p>Only called if 'shouldProxy()' returns true
   */
  public abstract Collection<String> getDestinationNodes();

  public abstract SolrRequest<?> prepareProxiedRequest();

  public abstract void processProxiedResponse(String nodeName, NamedList<Object> proxiedResponse);

  public boolean proxyRequest() {
    if (!shouldProxy()) {
      return false;
    }

    final var nodesToProxyTo = getDestinationNodes();
    final var solrRequest = prepareProxiedRequest();
    final var responseFutures = doProxyToNodes(nodesToProxyTo, solrRequest);
    bulkProcessResponses(responseFutures);
    return true;
  }

  // TODO Should we make either the request-submission or waiting timeout more configurable by
  // sub-classes?
  private void bulkProcessResponses(Map<String, Future<NamedList<Object>>> responseFutures) {
    for (Map.Entry<String, Future<NamedList<Object>>> entry : responseFutures.entrySet()) {
      try {
        NamedList<Object> resp = entry.getValue().get(10, TimeUnit.SECONDS);
        processProxiedResponse(entry.getKey(), resp);
      } catch (ExecutionException ee) {
        log.warn(
            "Exception when fetching result from node {}", entry.getKey(), ee.getCause()); // nowarn
      } catch (TimeoutException te) {
        log.warn("Timeout when fetching result from node {}", entry.getKey());
      } catch (InterruptedException e) {
        log.warn("Interrupted when fetching result from node {}", entry.getKey());
        Thread.currentThread().interrupt();
        break; // stop early
      }
    }
    if (log.isDebugEnabled()) {
      log.debug(
          "Fetched response from {} nodes: {}", responseFutures.size(), responseFutures.keySet());
    }
  }

  public static AdminHandlersProxy create(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    final var wtValue = req.getParams().get(WT);
    if ("prometheus".equals(wtValue) || "openmetrics".equals(wtValue)) {
      return new PrometheusRequestProxy(coreContainer, req, rsp);
    }

    return new NormalV1RequestProxy(coreContainer, req, rsp);
  }

  public static SolrRequest<?> createGenericRequest(String apiPath, SolrParams params) {
    return new GenericSolrRequest(SolrRequest.METHOD.GET, apiPath, params);
  }

  private Map<String, Future<NamedList<Object>>> doProxyToNodes(
      Collection<String> nodesToProxyTo, SolrRequest<?> solrRequest) {
    Map<String, Future<NamedList<Object>>> responses = new LinkedHashMap<>();
    for (String node : nodesToProxyTo) {
      responses.put(node, callRemoteNode(node, solrRequest));
    }
    return responses;
  }

  /** Makes a remote request asynchronously. */
  public CompletableFuture<NamedList<Object>> callRemoteNode(
      String nodeName, SolrRequest<?> solrRequest) {

    final var zkController = coreContainer.getZkController();
    // Validate that the node exists in the cluster
    if (!zkController.zkStateReader.getClusterState().getLiveNodes().contains(nodeName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Requested node " + nodeName + " is not part of cluster");
    }

    log.debug("Proxying {} request to node {}", solrRequest, nodeName);
    URI baseUri = URI.create(zkController.zkStateReader.getBaseUrlForNodeName(nodeName));

    try {
      return zkController
          .getCoreContainer()
          .getDefaultHttpSolrClient()
          .requestWithBaseUrl(baseUri.toString(), c -> c.requestAsync(solrRequest));
    } catch (SolrServerException | IOException e) {
      // requestWithBaseUrl declares it throws these but it actually depends on the lambda
      assert false : "requestAsync doesn't throw; it returns a Future";
      throw new RuntimeException(e);
    }
  }

  /**
   * Resolve node names from the "nodes" parameter into a set of live node names.
   *
   * @param nodeNames the value of the "nodes" parameter ("all" or comma-separated node names)
   * @param container the CoreContainer
   * @return set of resolved node names
   * @throws SolrException if node format is invalid
   */
  protected static Set<String> resolveNodes(String nodeNames, CoreContainer container) {
    Set<String> liveNodes =
        container.getZkController().zkStateReader.getClusterState().getLiveNodes();

    if (nodeNames.equals("all")) {
      log.debug("All live nodes requested");
      return liveNodes;
    }

    Set<String> nodes = new HashSet<>(Arrays.asList(nodeNames.split(",")));
    for (String nodeName : nodes) {
      if (!nodeName.matches("^[^/:]+:\\d+_[\\w/]+$")) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Parameter " + PARAM_NODES + " has wrong format");
      }
    }
    log.debug("Nodes requested: {}", nodes);
    return nodes;
  }
}
