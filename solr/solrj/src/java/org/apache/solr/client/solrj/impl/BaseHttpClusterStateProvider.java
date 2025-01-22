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

package org.apache.solr.client.solrj.impl;

import static org.apache.solr.client.solrj.SolrClient.RemoteSolrException;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseHttpClusterStateProvider implements ClusterStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String urlScheme;
  private List<URL> configuredNodes;
  volatile Set<String> liveNodes;
  long liveNodesTimestamp = 0;
  volatile Map<String, List<String>> aliases;
  volatile Map<String, Map<String, String>> aliasProperties;
  long aliasesTimestamp = 0;

  // the liveNodes and aliases cache will be invalidated after 5 secs
  private int cacheTimeout = EnvUtils.getPropertyAsInteger("solr.solrj.cache.timeout.sec", 5);

  public void init(List<String> solrUrls) throws Exception {
    this.configuredNodes =
        solrUrls.stream()
            .map(
                (solrUrl) -> {
                  try {
                    return new URI(solrUrl).toURL();
                  } catch (MalformedURLException | URISyntaxException e) {
                    throw new IllegalArgumentException(
                        "Failed to parse base Solr URL " + solrUrl, e);
                  }
                })
            .collect(Collectors.toList());

    for (String solrUrl : solrUrls) {
      urlScheme = solrUrl.startsWith("https") ? "https" : "http";
      try (SolrClient initialClient = getSolrClient(solrUrl)) {
        this.liveNodes = fetchLiveNodes(initialClient);
        liveNodesTimestamp = System.nanoTime();
        break;
      } catch (SolrServerException | IOException e) {
        log.warn("Attempt to fetch cluster state from {} failed.", solrUrl, e);
      }
    }

    if (this.liveNodes == null || this.liveNodes.isEmpty()) {
      throw new RuntimeException(
          "Tried fetching live_nodes using Solr URLs provided, i.e. "
              + solrUrls
              + ". However, "
              + "succeeded in obtaining the cluster state from none of them."
              + "If you think your Solr cluster is up and is accessible,"
              + " you could try re-creating a new CloudSolrClient using working"
              + " solrUrl(s).");
    }
  }

  /** Create a SolrClient implementation that uses the specified Solr node URL */
  protected abstract SolrClient getSolrClient(String baseUrl);

  @Override
  public DocCollection getCollection(String collection) {
    // This change is to prevent BaseHttpCSP make a call to fetch the entire cluster state, as the
    // default implementation calls getClusterState().getCollectionOrNull(name)
    return getState(collection).get();
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    for (String nodeName : liveNodes) {
      String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
      try (SolrClient client = getSolrClient(baseUrl)) {
        DocCollection docCollection = fetchCollectionState(client, collection);
        return new ClusterState.CollectionRef(docCollection);
      } catch (SolrServerException | IOException e) {
        log.warn(
            "Attempt to fetch cluster state from {} failed.",
            Utils.getBaseUrlForNodeName(nodeName, urlScheme),
            e);
      } catch (RemoteSolrException e) {
        if ("NOT_FOUND".equals(e.getMetadata("CLUSTERSTATUS"))) {
          return null;
        }
        log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
      } catch (NotACollectionException e) {
        // Cluster state for the given collection was not found, could be an alias.
        // Let's fetch/update our aliases:
        getAliases(true);
        return null;
      }
    }
    throw new RuntimeException(
        "Tried fetching cluster state using the node names we knew of, i.e. "
            + liveNodes
            + ". However, "
            + "succeeded in obtaining the cluster state from none of them."
            + "If you think your Solr cluster is up and is accessible,"
            + " you could try re-creating a new CloudSolrClient using working"
            + " solrUrl(s).");
  }

  @SuppressWarnings("unchecked")
  private ClusterState fetchClusterState(SolrClient client)
      throws SolrServerException, IOException, NotACollectionException {
    SimpleOrderedMap<?> cluster =
        submitClusterStateRequest(client, null, ClusterStateRequestType.FETCH_CLUSTER_STATE);

    List<String> liveNodesList = (List<String>) cluster.get("live_nodes");
    if (liveNodesList != null) {
      this.liveNodes = Set.copyOf(liveNodesList);
      liveNodesTimestamp = System.nanoTime();
    }

    var collectionsNl = (NamedList<Map<String, Object>>) cluster.get("collections");

    Map<String, DocCollection> collStateByName =
        CollectionUtil.newLinkedHashMap(collectionsNl.size());
    for (Entry<String, Map<String, Object>> entry : collectionsNl) {
      collStateByName.put(
          entry.getKey(), getDocCollectionFromObjects(entry.getKey(), entry.getValue()));
    }

    return new ClusterState(this.liveNodes, collStateByName);
  }

  @SuppressWarnings("unchecked")
  private DocCollection getDocCollectionFromObjects(
      String collectionName, Map<String, Object> collStateMap) {
    collStateMap.remove("health");

    int zNodeVersion = (int) collStateMap.remove("znodeVersion");

    Long creationTimeMillis = (Long) collStateMap.remove("creationTimeMillis");
    Instant creationTime =
        creationTimeMillis == null ? Instant.EPOCH : Instant.ofEpochMilli(creationTimeMillis);

    DocCollection.PrsSupplier prsSupplier = null;
    Map<String, Object> prs = (Map<String, Object>) collStateMap.remove("PRS");
    if (prs != null) {
      prsSupplier =
          () ->
              new PerReplicaStates(
                  (String) prs.get("path"),
                  (Integer) prs.get("cversion"),
                  (List<String>) prs.get("states"));
    }

    return ClusterState.collectionFromObjects(
        collectionName, collStateMap, zNodeVersion, creationTime, prsSupplier);
  }

  @SuppressWarnings("unchecked")
  private DocCollection fetchCollectionState(SolrClient client, String collection)
      throws SolrServerException, IOException, NotACollectionException {

    SimpleOrderedMap<?> cluster =
        submitClusterStateRequest(client, collection, ClusterStateRequestType.FETCH_COLLECTION);

    var collStateMap = (Map<String, Object>) cluster.findRecursive("collections", collection);
    if (collStateMap == null) {
      throw new NotACollectionException(); // probably an alias
    }
    return getDocCollectionFromObjects(collection, collStateMap);
  }

  private SimpleOrderedMap<?> submitClusterStateRequest(
      SolrClient client, String collection, ClusterStateRequestType requestType)
      throws SolrServerException, IOException {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "CLUSTERSTATUS");

    params.set("includeAll", false); // will flip flor CLUSTER_STATE
    switch (requestType) {
      case FETCH_CLUSTER_STATE -> params.set("includeAll", true);
      case FETCH_COLLECTION -> {
        if (collection != null) params.set("collection", collection);
      }
      case FETCH_LIVE_NODES -> params.set("liveNodes", true);
      case FETCH_CLUSTER_PROP -> params.set("clusterProperties", true);
      case FETCH_NODE_ROLES -> params.set("roles", true);
    }
    params.set("prs", true);
    var request = new GenericSolrRequest(METHOD.GET, "/admin/collections", params);
    return (SimpleOrderedMap<?>) client.request(request).get("cluster");
  }

  @Override
  public Set<String> getLiveNodes() {
    if (TimeUnit.SECONDS.convert((System.nanoTime() - liveNodesTimestamp), TimeUnit.NANOSECONDS)
        > getCacheTimeout()) {

      if (liveNodes.stream()
          .anyMatch((node) -> updateLiveNodes(URLUtil.getBaseUrlForNodeName(node, urlScheme))))
        return this.liveNodes;

      log.warn(
          "Attempt to fetch cluster state from all known live nodes {} failed. Trying backup nodes",
          liveNodes);

      if (configuredNodes.stream().anyMatch((node) -> updateLiveNodes(node.toString())))
        return this.liveNodes;

      throw new RuntimeException(
          "Tried fetching live_nodes using all the node names we knew of, i.e. "
              + liveNodes
              + ". However, "
              + "succeeded in obtaining the cluster state from none of them."
              + "If you think your Solr cluster is up and is accessible,"
              + " you could try re-creating a new CloudSolrClient using working"
              + " solrUrl(s).");
    } else {
      return this.liveNodes; // cached copy is fresh enough
    }
  }

  private boolean updateLiveNodes(String liveNode) {
    try (SolrClient client = getSolrClient(liveNode)) {
      this.liveNodes = fetchLiveNodes(client);
      liveNodesTimestamp = System.nanoTime();
      return true;
    } catch (Exception e) {
      log.warn("Attempt to fetch cluster state from {} failed.", liveNode, e);
    }
    return false;
  }

  @SuppressWarnings({"unchecked"})
  private Set<String> fetchLiveNodes(SolrClient client) throws Exception {
    SimpleOrderedMap<?> cluster =
        submitClusterStateRequest(client, null, ClusterStateRequestType.FETCH_LIVE_NODES);
    return Set.copyOf((List<String>) cluster.get("live_nodes"));
  }

  @Override
  public List<String> resolveAlias(String aliasName) {
    return resolveAlias(aliasName, false);
  }

  public List<String> resolveAlias(String aliasName, boolean forceFetch) {
    return Aliases.resolveAliasesGivenAliasMap(getAliases(forceFetch), aliasName);
  }

  @Override
  public String resolveSimpleAlias(String aliasName) throws IllegalArgumentException {
    return Aliases.resolveSimpleAliasGivenAliasMap(getAliases(false), aliasName);
  }

  private Map<String, List<String>> getAliases(boolean forceFetch) {
    if (this.liveNodes == null) {
      throw new RuntimeException(
          "We don't know of any live_nodes to fetch the"
              + " latest aliases information from. "
              + "If you think your Solr cluster is up and is accessible,"
              + " you could try re-creating a new CloudSolrClient using working"
              + " solrUrl(s).");
    }

    if (forceFetch
        || this.aliases == null
        || TimeUnit.SECONDS.convert((System.nanoTime() - aliasesTimestamp), TimeUnit.NANOSECONDS)
            > getCacheTimeout()) {
      for (String nodeName : liveNodes) {
        String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
        try (SolrClient client = getSolrClient(baseUrl)) {

          CollectionAdminResponse response =
              new CollectionAdminRequest.ListAliases().process(client);
          this.aliases = response.getAliasesAsLists();
          this.aliasProperties = response.getAliasProperties(); // side-effect
          this.aliasesTimestamp = System.nanoTime();
          return Collections.unmodifiableMap(this.aliases);
        } catch (SolrServerException | RemoteSolrException | IOException e) {
          // Situation where we're hitting an older Solr which doesn't have LISTALIASES
          if (e instanceof RemoteSolrException && ((RemoteSolrException) e).code() == 400) {
            log.warn(
                "LISTALIASES not found, possibly using older Solr server. Aliases won't work {}",
                "unless you upgrade Solr server",
                e);
            this.aliases = Collections.emptyMap();
            this.aliasProperties = Collections.emptyMap();
            this.aliasesTimestamp = System.nanoTime();
            return aliases;
          }
          log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
        }
      }

      throw new RuntimeException(
          "Tried fetching aliases using all the node names we knew of, i.e. "
              + liveNodes
              + ". However, "
              + "succeeded in obtaining the cluster state from none of them."
              + "If you think your Solr cluster is up and is accessible,"
              + " you could try re-creating a new CloudSolrClient using a working"
              + " solrUrl.");
    } else {
      return Collections.unmodifiableMap(this.aliases); // cached copy is fresh enough
    }
  }

  @Override
  public Map<String, String> getAliasProperties(String alias) {
    getAliases(false);
    return Collections.unmodifiableMap(aliasProperties.getOrDefault(alias, Collections.emptyMap()));
  }

  @Override
  public ClusterState getClusterState() {
    for (String nodeName : liveNodes) {
      String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
      try (SolrClient client = getSolrClient(baseUrl)) {
        return fetchClusterState(client);
      } catch (SolrServerException | RemoteSolrException | IOException e) {
        log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
      } catch (NotACollectionException e) {
        // not possible! (we passed in null for collection, so it can't be an alias)
        throw new RuntimeException(
            "null should never cause NotACollectionException in "
                + "fetchClusterState() Please report this as a bug!");
      }
    }
    throw new RuntimeException(
        "Tried fetching cluster state using the node names we knew of, i.e. "
            + liveNodes
            + ". However, "
            + "succeeded in obtaining the cluster state from none of them."
            + "If you think your Solr cluster is up and is accessible,"
            + " you could try re-creating a new CloudSolrClient using working"
            + " solrUrl(s).");
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> getClusterProperties() {
    for (String nodeName : liveNodes) {
      String baseUrl = Utils.getBaseUrlForNodeName(nodeName, urlScheme);
      try (SolrClient client = getSolrClient(baseUrl)) {
        SimpleOrderedMap<?> cluster =
            submitClusterStateRequest(client, null, ClusterStateRequestType.FETCH_CLUSTER_PROP);
        return (Map<String, Object>) cluster.get("properties");
      } catch (SolrServerException | RemoteSolrException | IOException e) {
        log.warn("Attempt to fetch cluster state from {} failed.", baseUrl, e);
      }
    }
    throw new RuntimeException(
        "Tried fetching cluster state using the node names we knew of, i.e. "
            + liveNodes
            + ". However, "
            + "succeeded in obtaining the cluster state from none of them."
            + "If you think your Solr cluster is up and is accessible,"
            + " you could try re-creating a new CloudSolrClient using working"
            + " solrUrl(s).");
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    throw new UnsupportedOperationException(
        "Fetching cluster properties not supported"
            + " using the HttpClusterStateProvider. "
            + "ZkClientClusterStateProvider can be used for this."); // TODO
  }

  @Override
  public Object getClusterProperty(String propertyName) {
    if (propertyName.equals(ClusterState.URL_SCHEME)) {
      return this.urlScheme;
    }
    return getClusterProperties().get(propertyName);
  }

  @Override
  public void connect() {}

  public int getCacheTimeout() {
    return cacheTimeout;
  }

  // This exception is not meant to escape this class it should be caught and wrapped.
  private static class NotACollectionException extends Exception {}

  @Override
  public String getQuorumHosts() {
    if (this.liveNodes == null) {
      return null;
    }
    return String.join(",", this.liveNodes);
  }

  private enum ClusterStateRequestType {
    FETCH_LIVE_NODES,
    FETCH_CLUSTER_PROP,
    FETCH_NODE_ROLES,
    FETCH_COLLECTION,
    FETCH_CLUSTER_STATE
  }
}
