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

package org.apache.solr.servlet;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.UnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.eclipse.jetty.client.HttpClient;

/**
 * If the request carries the {@link #TARGET_NODE_HEADER} header naming a different live node,
 * proxies the request there verbatim and returns; otherwise passes the request through. Unlike
 * {@link HttpSolrCall}'s collection/core-driven proxying, this works for any endpoint, including
 * node-level ones (e.g. {@code /solr/admin/**}, {@code /api/node/**}), so a request can be pinned
 * to a specific node even when reached through a load balancer that hides which node answers.
 */
public class NodeTargetProxyFilter extends CoreContainerAwareHttpFilter {

  /**
   * Header naming a live node (as it appears in ZK live_nodes) to proxy this request to. Two
   * shorthand forms are also accepted, both resolved relative to the current node: a value of all
   * digits replaces the trailing {@code -<digits>} ordinal in the current node's hostname (e.g.
   * {@code 8} turns {@code myhost-3.mydomain.com:3000} into {@code myhost-8.mydomain.com:3000});
   * otherwise, a value with no '.' is treated as a bare peer hostname, with the current node's
   * domain suffix and port appended.
   */
  public static final String TARGET_NODE_HEADER = "X-Solr-targetNode";

  @Override
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    // also accept it as a query param on GET requests, for convenience when a header can't
    // easily be set (e.g. a browser address bar)
    String targetNode = req.getHeader(TARGET_NODE_HEADER);
    String queryString = req.getQueryString();
    if (targetNode == null
        && "GET".equals(req.getMethod())
        && queryString != null
        && queryString.contains(TARGET_NODE_HEADER)) {
      targetNode = SolrRequestParsers.parseQueryString(queryString).get(TARGET_NODE_HEADER);
    }
    if (targetNode == null || targetNode.isBlank() || !proxyToNode(targetNode, req, res)) {
      chain.doFilter(req, res);
    }
  }

  /** Returns true if the request was proxied to {@code targetNode}. */
  private boolean proxyToNode(String targetNode, HttpServletRequest req, HttpServletResponse res) {
    CoreContainer cores;
    try {
      cores = getCores();
    } catch (UnavailableException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Core Container Unavailable");
    }
    if (!cores.isZooKeeperAware()) {
      // note: we could adapt this filter to support standalone mode with AllowListUrlChecker
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          TARGET_NODE_HEADER + " header/param requires SolrCloud");
    }
    String ownNodeName = cores.getZkController().getNodeName();
    String resolvedNode = resolvePeerShorthand(targetNode, ownNodeName);
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    if (!zkStateReader.getClusterState().getLiveNodes().contains(resolvedNode)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Unknown or non-live node: " + resolvedNode);
    }
    if (resolvedNode.equals(ownNodeName)) {
      return false; // already on the target node; let it process normally
    }

    String url =
        zkStateReader.getBaseUrlForNodeName(resolvedNode) + ServletUtils.getPathAfterContext(req);
    String queryString = req.getQueryString();
    if (queryString != null) {
      url = url + "?" + queryString;
    }
    try {
      HttpClient httpClient =
          ((HttpJettySolrClient) cores.getDefaultHttpSolrClient()).getHttpClient();
      HttpSolrProxy.doHttpProxy(httpClient, req, res, url);
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error proxying request to node: " + resolvedNode,
          e);
    } catch (Error e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  /**
   * If {@code targetNode} is all digits, it's an ordinal shorthand: {@code ownNodeName}'s first
   * hostname label is expected to end in {@code -<digits>}, which gets replaced. Otherwise, if it
   * has no '.' or ':', it's a bare peer hostname; expand it using {@code ownNodeName}'s domain
   * suffix and {@code :port_context}. Otherwise, returned unchanged.
   */
  static String resolvePeerShorthand(String targetNode, String ownNodeName) {
    if (targetNode.contains(".") || targetNode.contains(":")) {
      return targetNode; // already a full node name
    }
    int colonIdx = ownNodeName.indexOf(':');
    String ownHost = ownNodeName.substring(0, colonIdx);
    String portAndContext = ownNodeName.substring(colonIdx);
    int dotIdx = ownHost.indexOf('.');
    String domainSuffix = dotIdx >= 0 ? ownHost.substring(dotIdx) : "";

    if (targetNode.matches("\\d+")) {
      String firstLabel = dotIdx >= 0 ? ownHost.substring(0, dotIdx) : ownHost;
      targetNode = firstLabel.replaceFirst("-\\d+$", "-" + targetNode);
    }

    return targetNode + domainSuffix + portAndContext;
  }
}
