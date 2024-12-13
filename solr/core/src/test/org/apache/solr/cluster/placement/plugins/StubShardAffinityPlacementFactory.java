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

package org.apache.solr.cluster.placement.plugins;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeFetcherForTest;
import org.apache.solr.cluster.placement.BalancePlanFactory;
import org.apache.solr.cluster.placement.CollectionMetrics;
import org.apache.solr.cluster.placement.NodeMetric;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlanFactory;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.impl.AttributeValuesImpl;
import org.apache.solr.cluster.placement.impl.NodeMetricImpl;

public class StubShardAffinityPlacementFactory extends AffinityPlacementFactory {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new AffinityPlacementPlugin(
        config.minimalFreeDiskGB,
        config.prioritizedFreeDiskGB,
        config.withCollection,
        config.withCollectionShards,
        config.collectionNodeType,
        false) {
      @Override
      public List<PlacementPlan> computePlacements(
          Collection<PlacementRequest> requests, PlacementContext placementContext)
          throws PlacementException {
        final Map<String, Map<Node, String>> sysprops = new HashMap<>();
        final Map<NodeMetric<?>, Map<Node, Object>> metrics = new HashMap<>();
        final Map<String, CollectionMetrics> collectionMetrics = new HashMap<>();

        for (Node node : placementContext.getCluster().getLiveNodes()) {
          metrics.computeIfAbsent(NodeMetricImpl.NUM_CORES, n -> new HashMap<>()).put(node, 1);
          metrics
              .computeIfAbsent(NodeMetricImpl.FREE_DISK_GB, n -> new HashMap<>())
              .put(node, (double) 10);
          metrics
              .computeIfAbsent(NodeMetricImpl.TOTAL_DISK_GB, n -> new HashMap<>())
              .put(node, (double) 100);
        }
        final PlacementContext wrappingContext;
        wrappingContext =
            new PlacementContext() {

              private AttributeFetcherForTest fetcherForTest =
                  new AttributeFetcherForTest(
                      new AttributeValuesImpl(sysprops, metrics, collectionMetrics));

              @Override
              public Cluster getCluster() {
                return placementContext.getCluster();
              }

              @Override
              public AttributeFetcher getAttributeFetcher() {
                return fetcherForTest;
              }

              @Override
              public PlacementPlanFactory getPlacementPlanFactory() {
                return placementContext.getPlacementPlanFactory();
              }

              @Override
              public BalancePlanFactory getBalancePlanFactory() {
                return placementContext.getBalancePlanFactory();
              }
            };
        return super.computePlacements(requests, wrappingContext);
      }
    };
  }
}
