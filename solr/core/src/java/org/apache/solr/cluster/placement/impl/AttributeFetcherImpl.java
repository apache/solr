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

package org.apache.solr.cluster.placement.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.NodeValueFetcher;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.CollectionMetrics;
import org.apache.solr.cluster.placement.NodeMetric;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.common.cloud.Replica;

/**
 * Implementation of {@link AttributeFetcher} that uses {@link SolrCloudManager} to access Solr
 * cluster details.
 */
public class AttributeFetcherImpl implements AttributeFetcher {

  Set<String> requestedNodeSystemSnitchTags = new HashSet<>();
  Set<NodeMetric<?>> requestedNodeMetricSnitchTags = new HashSet<>();
  Map<SolrCollection, Set<ReplicaMetric<?>>> requestedCollectionMetrics = new HashMap<>();

  Set<Node> nodes = Collections.emptySet();

  private final SolrCloudManager cloudManager;

  AttributeFetcherImpl(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
  }

  @Override
  public AttributeFetcher requestNodeSystemProperty(String name) {
    requestedNodeSystemSnitchTags.add(getSystemPropertySnitchTag(name));
    return this;
  }

  @Override
  public AttributeFetcher requestNodeMetric(NodeMetric<?> metric) {
    requestedNodeMetricSnitchTags.add(metric);
    return this;
  }

  @Override
  public AttributeFetcher requestCollectionMetrics(
      SolrCollection solrCollection, Set<ReplicaMetric<?>> metrics) {
    if (!metrics.isEmpty()) {
      requestedCollectionMetrics.put(solrCollection, Set.copyOf(metrics));
    }
    return this;
  }

  @Override
  public AttributeFetcher fetchFrom(Set<Node> nodes) {
    this.nodes = nodes;
    return this;
  }

  // TODO: This method is overly complex and very confusing for trying to get metrics across
  // nodes. Probably because of the complex filtering parameters of Dropwizard and trying to collect
  // system property strings + ints + longs in a single map and have a conversion method in the
  // middle of it.
  // With migration to OTEL, we should come back and clean up this so it is not so confusing of
  // trying to link multiple maps with one another with a much cleaner implementation
  @Override
  public AttributeValues fetchAttributes() {

    // Maps in which attribute values will be added
    Map<String, Map<Node, String>> systemSnitchToNodeToValue = new HashMap<>();
    Map<NodeMetric<?>, Map<Node, Object>> metricSnitchToNodeToValue = new HashMap<>();
    Map<String, CollectionMetricsBuilder> collectionMetricsBuilders = new HashMap<>();
    Map<Node, Set<String>> nodeToReplicaInternalTags = new HashMap<>();
    Map<String, Set<ReplicaMetric<?>>> requestedCollectionNamesMetrics =
        requestedCollectionMetrics.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue()));

    // In order to match the returned values for the various snitches, we need to keep track of
    // where each received value goes. Given the target maps are of different types (the maps from
    // Node to whatever defined above) we instead pass a function taking two arguments, the node and
    // the (non null) returned value, that will cast the value into the appropriate type for the
    // snitch tag and insert it into the appropriate map with the node as the key.
    for (String sysPropSnitch : requestedNodeSystemSnitchTags) {
      systemSnitchToNodeToValue.put(sysPropSnitch, new HashMap<>());
    }
    for (NodeMetric<?> metric : requestedNodeMetricSnitchTags) {
      metricSnitchToNodeToValue.put(metric, new HashMap<>());
    }
    requestedCollectionMetrics.forEach(
        (collection, tags) -> {
          Set<String> collectionTags =
              tags.stream().map(tag -> tag.getInternalName()).collect(Collectors.toSet());
          collection
              .shards()
              .forEach(
                  shard ->
                      shard
                          .replicas()
                          .forEach(
                              replica -> {
                                Set<String> perNodeInternalTags =
                                    nodeToReplicaInternalTags.computeIfAbsent(
                                        replica.getNode(), n -> new HashSet<>());
                                perNodeInternalTags.addAll(collectionTags);
                              }));
        });

    final NodeStateProvider nodeStateProvider = cloudManager.getNodeStateProvider();

    // Now that we know everything we need to fetch (and where to put it), just do it.
    // TODO: we could probably fetch this in parallel - for large clusters this could
    // significantly shorten the execution time
    for (Node node : nodes) {
      // Fetch system properties and metrics for nodes
      if (!requestedNodeSystemSnitchTags.isEmpty() || !requestedNodeMetricSnitchTags.isEmpty()) {
        fetchNodeValues(
            node, nodeStateProvider, systemSnitchToNodeToValue, metricSnitchToNodeToValue);
      }
    }

    for (Node node : nodeToReplicaInternalTags.keySet()) {
      Set<String> tags = nodeToReplicaInternalTags.get(node);
      Map<String, Map<String, List<Replica>>> infos =
          nodeStateProvider.getReplicaInfo(node.getName(), tags);
      infos.entrySet().stream()
          .filter(entry -> requestedCollectionNamesMetrics.containsKey(entry.getKey()))
          .forEach(
              entry -> {
                CollectionMetricsBuilder collectionMetricsBuilder =
                    collectionMetricsBuilders.computeIfAbsent(
                        entry.getKey(), c -> new CollectionMetricsBuilder());
                entry
                    .getValue()
                    .forEach(
                        (shardName, replicas) -> {
                          CollectionMetricsBuilder.ShardMetricsBuilder shardMetricsBuilder =
                              collectionMetricsBuilder
                                  .getShardMetricsBuilders()
                                  .computeIfAbsent(
                                      shardName,
                                      s -> new CollectionMetricsBuilder.ShardMetricsBuilder(s));
                          replicas.forEach(
                              replica -> {
                                CollectionMetricsBuilder.ReplicaMetricsBuilder
                                    replicaMetricsBuilder =
                                        shardMetricsBuilder
                                            .getReplicaMetricsBuilders()
                                            .computeIfAbsent(
                                                replica.getName(),
                                                n ->
                                                    new CollectionMetricsBuilder
                                                        .ReplicaMetricsBuilder(n));
                                replicaMetricsBuilder.setLeader(replica.isLeader());
                                if (replica.isLeader()) {
                                  shardMetricsBuilder.setLeaderMetrics(replicaMetricsBuilder);
                                }
                                Set<ReplicaMetric<?>> requestedMetrics =
                                    requestedCollectionNamesMetrics.get(replica.getCollection());
                                requestedMetrics.forEach(
                                    metric -> {
                                      replicaMetricsBuilder.addMetric(
                                          metric, replica.get(metric.getInternalName()));
                                    });
                              });
                        });
              });
    }

    Map<String, CollectionMetrics> collectionMetrics = new HashMap<>();
    collectionMetricsBuilders.forEach(
        (name, builder) -> collectionMetrics.put(name, builder.build()));

    return new AttributeValuesImpl(
        systemSnitchToNodeToValue, metricSnitchToNodeToValue, collectionMetrics);
  }

  /**
   * Fetch both system properties and node metric values for a specific node and add it accordingly
   * to the maps
   */
  private void fetchNodeValues(
      Node node,
      NodeStateProvider nodeStateProvider,
      Map<String, Map<Node, String>> systemSnitchToNodeToValue,
      Map<NodeMetric<?>, Map<Node, Object>> metricSnitchToNodeToValue) {

    Set<String> allRequestedTags = new HashSet<>();
    Map<String, NodeMetric<?>> tagToMetric = new HashMap<>();

    // Add system property tags we need to request
    allRequestedTags.addAll(requestedNodeSystemSnitchTags);

    // Add metric tags we need to request
    for (NodeMetric<?> metric : requestedNodeMetricSnitchTags) {
      String tag = buildMetricTag(metric);
      allRequestedTags.add(tag);
      tagToMetric.put(tag, metric);
    }

    if (allRequestedTags.isEmpty()) {
      return;
    }

    // Fetch all system properties and metric values for the requested tags
    Map<String, Object> allValues =
        nodeStateProvider.getNodeValues(node.getName(), allRequestedTags);

    // Now process the results and place the system property and metric values in the correct maps
    for (Map.Entry<String, Object> entry : allValues.entrySet()) {
      String tag = entry.getKey();
      Object value = entry.getValue();

      if (value != null) {
        // Check if it's a system property
        if (requestedNodeSystemSnitchTags.contains(tag)) {
          systemSnitchToNodeToValue.get(tag).put(node, (String) value);
        }

        // Check if it's a metric
        NodeMetric<?> metric = tagToMetric.get(tag);
        if (metric != null) {
          Object convertedValue = metric.convert(value);
          metricSnitchToNodeToValue.get(metric).put(node, convertedValue);
        }
      }
    }
  }

  /**
   * Build a string tag for a NodeMetric that can be used with NodeStateProvider.getNodeValues().
   */
  private String buildMetricTag(NodeMetric<?> metric) {
    String metricTagName;
    if (NodeValueFetcher.tags.contains(metric.getInternalName())) {
      // "special" well-known tag
      metricTagName = metric.getInternalName();
    } else {
      // A full Prometheus metric name
      metricTagName = NodeValueFetcher.METRICS_PREFIX + metric.getInternalName();
    }
    // Append label to metricTagName to filter
    if (metric.hasLabels()) {
      metricTagName =
          metricTagName + ":" + metric.getLabelKey() + "=" + "\"" + metric.getLabelValue() + "\"";
    }
    return metricTagName;
  }

  public static String getSystemPropertySnitchTag(String name) {
    return NodeValueFetcher.SYSPROP + name;
  }
}
