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

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.common.SolrException.ErrorCode.SERVICE_UNAVAILABLE;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.UnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If the request carries the {@link #TARGET_NODE_HEADER} header naming a different live node,
 * proxies the request there verbatim and returns; otherwise passes the request through. Unlike
 * {@link HttpSolrCall}'s collection/core-driven proxying, this works for any endpoint, including
 * node-level ones (e.g. {@code /solr/admin/**}, {@code /api/node/**}), so a request can be pinned
 * to a specific node even when reached through a load balancer that hides which node answers.
 */
public class NodeTargetProxyFilter extends CoreContainerAwareHttpFilter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Header naming a live node (as it appears in ZK live_nodes) to proxy this request to. Two
   * shorthand forms are also accepted, both resolved relative to the current node: a value of all
   * digits replaces the trailing {@code -<digits>} ordinal in the current node's hostname (e.g.
   * {@code 8} turns {@code myhost-3.mydomain.com:3000} into {@code myhost-8.mydomain.com:3000});
   * otherwise, a value with no '.' or ':' is treated as a bare peer hostname, with the current node's
   * domain suffix and port appended.
   */
  public static final String TARGET_NODE_HEADER = "X-Solr-targetNode";

  @Override
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {

    String targetNode = req.getHeader(TARGET_NODE_HEADER);

    // also accept it as a query param on GET requests, for convenience when a header can't
    // easily be set (e.g. a browser address bar)
    String queryString = req.getQueryString();
    if ("GET".equals(req.getMethod())
        && queryString != null
        && queryString.contains(TARGET_NODE_HEADER)) { // cheap; avoid double-parse
      SolrParams params = SolrRequestParsers.parseQueryString(queryString);
      String paramTargetNode = params.get(TARGET_NODE_HEADER);
      if (paramTargetNode != null) {
        if (targetNode != null) {
          res.sendError(400, TARGET_NODE_HEADER + " must EITHER be a header or url param");
          return;
        }
        targetNode = paramTargetNode;
        queryString = filterQueryString(params);
      }
    }

    // wrap the request to filter out the header & param.
    //   NOTE: we assume downstream processing only accesses such data via what we override !!
    if (targetNode != null) {
      final List<String> headerNames = filterHeaderNamesToList(req); // cheap
      final String finalQueryString = queryString; // filtered already if needed
      req =
          new HttpServletRequestWrapper(req) {
            @Override
            public Enumeration<String> getHeaderNames() {
              return Collections.enumeration(headerNames);
            }

            @Override
            public String getQueryString() {
              return finalQueryString;
            }
          };
    }

    if (targetNode != null && !targetNode.isBlank() && proxyToNode(targetNode, req, res)) {
      return;
    }
    chain.doFilter(req, res);
  }

  private static List<String> filterHeaderNamesToList(HttpServletRequest req) {
    List<String> headerNames = new ArrayList<>();
    var headerNamesEnum = req.getHeaderNames();
    while (headerNamesEnum.hasMoreElements()) {
      String headerName = headerNamesEnum.nextElement();
      if (!headerName.equalsIgnoreCase(TARGET_NODE_HEADER)) {
        headerNames.add(headerName);
      }
    }
    return headerNames;
  }

  private static String filterQueryString(SolrParams params) {
    ModifiableSolrParams modifiableSolrParams = ModifiableSolrParams.of(params);
    modifiableSolrParams.remove(TARGET_NODE_HEADER);
    String queryString = modifiableSolrParams.toQueryString();
    if (queryString.startsWith("?")) {
      queryString = queryString.substring(1);
    }
    return queryString; // could be empty string
  }

  /**
   * Returns true if the request was handled here: proxied to {@code targetNode}, or answered with
   * an error. Reports errors itself, since an exception would be treated by {@link
   * AuthenticationFilter}, our caller, as an authentication failure.
   */
  private boolean proxyToNode(String targetNode, HttpServletRequest req, HttpServletResponse res)
      throws IOException {
    CoreContainer cores;
    try {
      cores = getCores();
    } catch (UnavailableException e) {
      res.sendError(SERVICE_UNAVAILABLE.code, "Core Container Unavailable");
      return true;
    }
    if (!cores.isZooKeeperAware()) {
      // note: we could adapt this filter to support standalone mode with AllowListUrlChecker
      res.sendError(BAD_REQUEST.code, TARGET_NODE_HEADER + " header/param requires SolrCloud");
      return true;
    }
    String ownNodeName = cores.getZkController().getNodeName();
    String resolvedNode = resolvePeerShorthand(targetNode, ownNodeName);
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    if (!zkStateReader.getClusterState().getLiveNodes().contains(resolvedNode)) {
      res.sendError(BAD_REQUEST.code, "Unknown or non-live node: " + resolvedNode);
      return true;
    }
    if (resolvedNode.equals(ownNodeName)) {
      return false; // already on the target node; let it process normally
    }

    URI uri;
    try {
      uri = targetUri(zkStateReader.getBaseUrlForNodeName(resolvedNode), req);
    } catch (SolrException e) {
      res.sendError(e.code(), e.getMessage());
      return true;
    } catch (IllegalArgumentException e) {
      res.sendError(BAD_REQUEST.code, "Invalid request URI");
      return true;
    }
    try {
      HttpClient httpClient =
          ((HttpJettySolrClient) cores.getDefaultHttpSolrClient()).getHttpClient();
      HttpSolrProxy.doHttpProxy(httpClient, req, res, uri);
    } catch (Exception e) {
      log.warn("Error proxying request to node {}", resolvedNode, e);
      res.sendError(SERVER_ERROR.code, "Error proxying request to node: " + resolvedNode);
    } catch (Error e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  /**
   * The request's URI on another node. The path is reused as received (still encoded). The query is
   * re-encoded because Jetty accepts characters in it, like '{' or '|', that a URI rejects.
   */
  private static URI targetUri(String baseUrl, HttpServletRequest req) {
    String query = req.getQueryString();
    query = query == null ? "" : SolrRequestParsers.parseQueryString(query).toQueryString();
    return URI.create(baseUrl + pathAfterContext(req) + query);
  }

  /** The still-encoded request path with the context path removed. Starts with "/". */
  private static String pathAfterContext(HttpServletRequest req) {
    return req.getRequestURI().substring(req.getContextPath().length());
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
