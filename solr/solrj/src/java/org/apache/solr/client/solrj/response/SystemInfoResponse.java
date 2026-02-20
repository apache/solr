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
package org.apache.solr.client.solrj.response;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.solr.client.api.model.NodeSystemResponse;
import org.apache.solr.client.solrj.request.json.JacksonContentWriter;
import org.apache.solr.common.util.NamedList;

/** This class holds the response from V1 "/admin/info/system" */
public class SystemInfoResponse extends SolrResponseBase {

  private static final long serialVersionUID = 1L;

  // AdminHandlersProxy wraps nodes responses in a map.
  // Mimic that here, even if the response might be just a single node.
  protected final Map<String, NodeSystemResponse.NodeSystemInfo> nodesInfo = new HashMap<>();

  protected SystemInfoResponse() {
    // required constructor for the V2 extension
  }

  public SystemInfoResponse(NamedList<Object> namedList) {
    if (namedList == null) throw new IllegalArgumentException("Null NamedList is not allowed.");
    setResponse(namedList);
  }

  @Override
  public void setResponse(NamedList<Object> response) {
    if (getResponse() == null) {
      super.setResponse(response);
      parseResponse(response);
    } else {
      assert response.equals(getResponse());
      return;
    }
  }

  /** Parse the V1 response */
  @SuppressWarnings("unchecked")
  protected void parseResponse(NamedList<Object> response) {
    if (response.get("node") == null) {
      // multi-nodes response, NamedList of "host:port_solr"-> NodeSystemResponse
      for (Entry<String, Object> node : response) {
        if (node.getKey().endsWith("_solr")) {
          nodesInfo.put(
              node.getKey(),
              JacksonContentWriter.DEFAULT_MAPPER.convertValue(
                  removeHeader((NamedList<Object>) node.getValue()),
                  NodeSystemResponse.NodeSystemInfo.class));
        }
      }

      // If no node was found, that's very likely Solr runs in standalone mode.
      // Add a single node info instance with null key (no node name is available).
      if (nodesInfo.isEmpty()) {
        nodesInfo.put(
            null,
            JacksonContentWriter.DEFAULT_MAPPER.convertValue(
                removeHeader(response), NodeSystemResponse.NodeSystemInfo.class));
      }

    } else {
      // single-node response
      nodesInfo.put(
          response.get("node").toString(),
          JacksonContentWriter.DEFAULT_MAPPER.convertValue(
              removeHeader(response), NodeSystemResponse.NodeSystemInfo.class));
    }
  }

  private NamedList<Object> removeHeader(NamedList<Object> value) {
    value.remove("responseHeader");
    return value;
  }

  /** Get the mode from a single node system info */
  public String getMode() {
    if (nodesInfo.size() == 1) {
      return nodesInfo.values().stream().findFirst().orElseThrow().mode;
    } else {
      throw new UnsupportedOperationException(
          "Multiple nodes system info available, use method 'getAllModes', or 'getModeForNode(String)'.");
    }
  }

  /** Get all modes, per node */
  public Map<String, String> getAllModes() {
    Map<String, String> allModes = new HashMap<>();
    nodesInfo.forEach((key, value) -> allModes.put(key, value.mode));
    return allModes;
  }

  /** Get the mode for the given node name */
  public String getModeForNode(String node) {
    return nodesInfo.get(node).mode;
  }

  /** Get the hostname from a single node system info */
  public String getHost() {
    if (nodesInfo.size() == 1) {
      return nodesInfo.values().stream().findFirst().orElseThrow().host;
    } else {
      throw new UnsupportedOperationException(
          "Multiple nodes system info available, use method 'getAllHosts', or 'getHostForNode(String)'.");
    }
  }

  /** Get all hostnames, per node */
  public Map<String, String> getAllHosts() {
    Map<String, String> allHosts = new HashMap<>();
    nodesInfo.forEach((key, value) -> allHosts.put(key, value.host));
    return allHosts;
  }

  /** Get the hostname for the given node name */
  public String getHostForNode(String node) {
    return nodesInfo.get(node).host;
  }

  /** Get the ZK host from a single node system info */
  public String getZkHost() {
    if (nodesInfo.size() == 1) {
      return nodesInfo.values().stream().findFirst().orElseThrow().zkHost;
    } else {
      throw new UnsupportedOperationException(
          "Multiple nodes system info available, use method 'getAllZkHosts', or 'getZkHostForNode(String)'.");
    }
  }

  /** Get all ZK hosts, per node */
  public Map<String, String> getAllZkHosts() {
    Map<String, String> allModes = new HashMap<>();
    nodesInfo.forEach((key, value) -> allModes.put(key, value.zkHost));
    return allModes;
  }

  /** Get the ZK host for the given node name */
  public String getZkHostForNode(String node) {
    return nodesInfo.get(node).zkHost;
  }

  /** Get the Solr home from a single node system info */
  public String getSolrHome() {
    if (nodesInfo.size() == 1) {
      return nodesInfo.values().stream().findFirst().orElseThrow().solrHome;
    } else {
      throw new UnsupportedOperationException(
          "Multiple nodes system info available, use method 'getAllSolrHomes', or 'getSolrHomeForNode(String)'.");
    }
  }

  /** Get all Solr homes, per node */
  public Map<String, String> getAllSolrHomes() {
    Map<String, String> allModes = new HashMap<>();
    nodesInfo.forEach((key, value) -> allModes.put(key, value.solrHome));
    return allModes;
  }

  /** Get the Solr home for the given node name */
  public String getSolrHomeForNode(String node) {
    return nodesInfo.get(node).solrHome;
  }

  /** Get the core root from a single node system info */
  public String getCoreRoot() {
    if (nodesInfo.size() == 1) {
      return nodesInfo.values().stream().findFirst().orElseThrow().coreRoot;
    } else {
      throw new UnsupportedOperationException(
          "Multiple nodes system info available, use method 'getAllCoreRoots', or 'getCoreRootForNode(String)'.");
    }
  }

  /** Get all core roots, per node */
  public Map<String, String> getAllCoreRoots() {
    Map<String, String> allModes = new HashMap<>();
    nodesInfo.forEach((key, value) -> allModes.put(key, value.coreRoot));
    return allModes;
  }

  /** Get the core root for the given node name */
  public String getCoreRootForNode(String node) {
    return nodesInfo.get(node).coreRoot;
  }

  /** Get the node name from a single node system info */
  public String getNode() {
    if (nodesInfo.size() == 1) {
      return nodesInfo.values().stream().findFirst().orElseThrow().node;
    } else {
      throw new UnsupportedOperationException(
          "Multiple nodes system info available, use method 'getAllNodes', or 'getNodeForSolrHome(String)', or 'getNodeForCoreRoot(String)'.");
    }
  }

  /** Get all nodes names */
  public Set<String> getAllNodes() {
    return nodesInfo.keySet();
  }

  /** Get the node name for the given Solr home */
  public String getNodeForSolrHome(String solrHome) {
    return nodesInfo.values().stream()
        .filter(v -> solrHome.equals(v.solrHome))
        .map(v -> v.node)
        .findFirst()
        .get();
  }

  /** Get the node name for the given core root */
  public String getNodeForCoreRoot(String coreRoot) {
    return nodesInfo.values().stream()
        .filter(v -> coreRoot.equals(v.coreRoot))
        .map(v -> v.node)
        .findFirst()
        .get();
  }

  /** Get the {@code NodeSystemResponse.NodeSystemInfo} for a single node */
  public NodeSystemResponse.NodeSystemInfo getNodeResponse() {
    if (nodesInfo.size() == 1) {
      return nodesInfo.values().stream().findFirst().get();
    } else {
      throw new UnsupportedOperationException(
          "Multiple nodes system info available, use method 'getAllNodeResponses', or 'getNodeResponseForNode(String)'.");
    }
  }

  /** Get all {@code NodeSystemResponse}s */
  public Map<String, NodeSystemResponse.NodeSystemInfo> getAllNodeResponses() {
    return nodesInfo;
  }

  /** Get the {@code NodeSystemResponse.NodeSystemInfo} for the given node name */
  public NodeSystemResponse.NodeSystemInfo getNodeResponseForNode(String node) {
    return nodesInfo.get(node);
  }

  public String getSolrImplVersion() {
    return getNodeResponse() != null && getNodeResponse().lucene != null
        ? getNodeResponse().lucene.solrImplVersion
        : null;
  }

  public String getSolrSpecVersion() {
    return getNodeResponse() != null && getNodeResponse().lucene != null
        ? getNodeResponse().lucene.solrSpecVersion
        : null;
  }

  public Date getJVMStartTime() {
    return getNodeResponse() != null
            && getNodeResponse().jvm != null
            && getNodeResponse().jvm.jmx != null
        ? getNodeResponse().jvm.jmx.startTime
        : null;
  }

  public Long getJVMUpTimeMillis() {
    return getNodeResponse() != null
            && getNodeResponse().jvm != null
            && getNodeResponse().jvm.jmx != null
        ? getNodeResponse().jvm.jmx.upTimeMS
        : null;
  }

  public String getHumanReadableJVMMemoryUsed() {
    return getNodeResponse() != null
            && getNodeResponse().jvm != null
            && getNodeResponse().jvm.memory != null
        ? getNodeResponse().jvm.memory.used
        : null;
  }

  public String getHumanReadableJVMMemoryTotal() {
    return getNodeResponse() != null
            && getNodeResponse().jvm != null
            && getNodeResponse().jvm.memory != null
        ? getNodeResponse().jvm.memory.total
        : null;
  }
}
