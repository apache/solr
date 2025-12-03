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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Arrays;
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
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
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
public class AdminHandlersProxy {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_NODES = "nodes";
  private static final String PARAM_NODE = "node";
  private static final long PROMETHEUS_FETCH_TIMEOUT_SECONDS = 10;

  /** Proxy this request to a different remote node if 'node' or 'nodes' parameter is provided */
  public static boolean maybeProxyToNodes(
      SolrQueryRequest req, SolrQueryResponse rsp, CoreContainer container)
      throws IOException, SolrServerException, InterruptedException {

    String pathStr = req.getPath();
    ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());

    // Check if response format is Prometheus/OpenMetrics
    String wt = params.get("wt");
    boolean isPrometheusFormat = "prometheus".equals(wt) || "openmetrics".equals(wt);

    if (isPrometheusFormat) {
      // Prometheus format: use singular 'node' parameter for single-node proxy
      String nodeName = req.getParams().get(PARAM_NODE);
      if (nodeName == null || nodeName.isEmpty()) {
        return false; // No node parameter, handle locally
      }

      params.remove(PARAM_NODE);
      handlePrometheusSingleNode(nodeName, pathStr, params, container, rsp);
    } else {
      // Other formats (JSON/XML): use plural 'nodes' parameter for multi-node aggregation
      String nodeNames = req.getParams().get(PARAM_NODES);
      if (nodeNames == null || nodeNames.isEmpty()) {
        return false; // No nodes parameter, handle locally
      }

      params.remove(PARAM_NODES);
      Set<String> nodes = resolveNodes(nodeNames, container);
      handleNamedListFormat(nodes, pathStr, params, container.getZkController(), rsp);
    }

    return true;
  }

  /** Handle non-Prometheus formats using the existing NamedList approach. */
  private static void handleNamedListFormat(
      Set<String> nodes,
      String pathStr,
      SolrParams params,
      ZkController zkController,
      SolrQueryResponse rsp) {

    Map<String, Future<NamedList<Object>>> responses = new LinkedHashMap<>();
    for (String node : nodes) {
      responses.put(node, callRemoteNode(node, pathStr, params, zkController));
    }

    for (Map.Entry<String, Future<NamedList<Object>>> entry : responses.entrySet()) {
      try {
        NamedList<Object> resp = entry.getValue().get(10, TimeUnit.SECONDS);
        rsp.add(entry.getKey(), resp);
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
      log.debug("Fetched response from {} nodes: {}", responses.size(), responses.keySet());
    }
  }

  /** Makes a remote request asynchronously. */
  public static CompletableFuture<NamedList<Object>> callRemoteNode(
      String nodeName, String uriPath, SolrParams params, ZkController zkController) {

    // Validate that the node exists in the cluster
    if (!zkController.zkStateReader.getClusterState().getLiveNodes().contains(nodeName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Requested node " + nodeName + " is not part of cluster");
    }

    log.debug("Proxying {} request to node {}", uriPath, nodeName);
    URI baseUri = URI.create(zkController.zkStateReader.getBaseUrlForNodeName(nodeName));
    SolrRequest<?> proxyReq = new GenericSolrRequest(SolrRequest.METHOD.GET, uriPath, params);

    // Set response parser based on wt parameter to ensure correct format is used
    String wt = params.get("wt");
    if ("prometheus".equals(wt) || "openmetrics".equals(wt)) {
      proxyReq.setResponseParser(new InputStreamResponseParser(wt));
    }

    try {
      return zkController
          .getCoreContainer()
          .getDefaultHttpSolrClient()
          .requestWithBaseUrl(baseUri.toString(), c -> c.requestAsync(proxyReq));
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
  private static Set<String> resolveNodes(String nodeNames, CoreContainer container) {
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

  /**
   * Handle Prometheus format by proxying to a single node. *
   *
   * @param nodeName the name of the single node to proxy to
   * @param pathStr the request path
   * @param params the request parameters (with 'node' parameter already removed)
   * @param container the CoreContainer
   * @param rsp the response to populate
   */
  private static void handlePrometheusSingleNode(
      String nodeName,
      String pathStr,
      ModifiableSolrParams params,
      CoreContainer container,
      SolrQueryResponse rsp)
      throws IOException, SolrServerException {

    // Keep wt=prometheus for the remote request so MetricsHandler accepts it
    // The InputStreamResponseParser will return the Prometheus text in a "stream" key
    Future<NamedList<Object>> response =
        callRemoteNode(nodeName, pathStr, params, container.getZkController());

    try {
      try {
        NamedList<Object> resp = response.get(PROMETHEUS_FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        rsp.getValues().addAll(resp);
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    } catch (IOException | SolrServerException | RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) { // unlikely?
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, t);
    }
  }
}
