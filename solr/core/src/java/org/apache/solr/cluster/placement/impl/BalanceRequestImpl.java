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

import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.BalanceRequest;
import org.apache.solr.cluster.placement.PlacementRequest;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

public class BalanceRequestImpl implements BalanceRequest {

  private final SolrCollection solrCollection;
  final private Set<Node> nodes;
  final private int maximumBalanceSkew;

  public BalanceRequestImpl(Set<Node> nodes) {
    this(nodes, -1);
  }

  public BalanceRequestImpl(Set<Node> nodes, int maximumBalanceSkew) {
    this.nodes = nodes;
    this.maximumBalanceSkew = maximumBalanceSkew;
    this.solrCollection = null;
  }

  @Override
  public Set<Node> getNodes() {
    return nodes;
  }

  @Override
  public int getMaximumBalanceSkew() {
    return maximumBalanceSkew;
  }

  @Override
  public SolrCollection getCollection() {
    return solrCollection;
  }

  /**
   * Returns a {@link PlacementRequest} that can be consumed by a plugin based on an internal
   * Assign.AssignRequest for adding replicas + additional info (upon creation of a new collection
   * or adding replicas to an existing one).
   */
  static BalanceRequestImpl toPlacementRequest(
      Cluster cluster, SolrCollection solrCollection, Assign.AssignRequest assignRequest)
      throws Assign.AssignmentException {
    final Set<Node> nodes;
    // If no nodes specified, use all live nodes. If nodes are specified, use specified list.
    if (assignRequest.nodes != null) {
      nodes = SimpleClusterAbstractionsImpl.NodeImpl.getNodes(assignRequest.nodes);

      for (Node n : nodes) {
        if (!cluster.getLiveDataNodes().contains(n)) {
          throw new Assign.AssignmentException(
              "Bad assign request: specified node is a non-data hosting node ("
                  + n.getName()
                  + ") for collection "
                  + solrCollection.getName());
        }
      }
      if (nodes.isEmpty()) {
        throw new Assign.AssignmentException(
            "Bad assign request: empty list of nodes for collection " + solrCollection.getName());
      }
    } else {
      nodes = cluster.getLiveDataNodes();
      if (nodes.isEmpty()) {
        throw new Assign.AssignmentException(
            "Impossible assign request: no live nodes for collection " + solrCollection.getName());
      }
    }

    return new BalanceRequestImpl(nodes);
  }
}
