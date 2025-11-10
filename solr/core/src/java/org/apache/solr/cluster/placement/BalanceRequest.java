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

package org.apache.solr.cluster.placement;

import java.util.Set;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;

/**
 * A cluster related placement request that Solr asks a {@link PlacementPlugin} to resolve and
 * compute replica balancing plan for replicas that already exist across a set of Nodes.
 *
 * <p>The set of {@link Node}s on which the replicas should be balanced across is specified
 * (defaults to being equal to the set returned by {@link Cluster#getLiveDataNodes()}).
 */
public interface BalanceRequest extends ModificationRequest {

  /**
   * Replicas should only be balanced on nodes in the set returned by this method.
   *
   * <p>When Collection API calls do not specify a specific set of nodes, replicas can be balanced
   * on all live nodes in the cluster. In such cases, this set will be equal to the set of all live
   * nodes. The plugin placement code does not need to worry (or care) if a set of nodes was
   * explicitly specified or not.
   *
   * @return never {@code null} and never empty set (if that set was to be empty for any reason, no
   *     balance would be possible and the Solr infrastructure driving the plugin code would detect
   *     the error itself rather than calling the plugin).
   */
  Set<Node> getNodes();

  int getMaximumBalanceSkew();
}
