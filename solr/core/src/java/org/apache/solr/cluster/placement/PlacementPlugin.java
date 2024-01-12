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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Implemented by external plugins to control replica placement and movement on the search cluster
 * (as well as other things such as cluster elasticity?) when cluster changes are required
 * (initiated elsewhere, most likely following a Collection API call).
 *
 * <p>Instances of classes implementing this interface are created by {@link PlacementPluginFactory}
 *
 * <p>Implementations of this interface <b>must</b> be reentrant. {@link #computePlacement}
 * <b>will</b> be called concurrently from many threads.
 */
public interface PlacementPlugin {
  /**
   * Request from plugin code to compute placement. Note this method must be reentrant as a plugin
   * instance may (read will) get multiple such calls in parallel.
   *
   * <p>Configuration is passed upon creation of a new instance of this class by {@link
   * PlacementPluginFactory#createPluginInstance}.
   *
   * @param placementRequest request for placing new replicas or moving existing replicas on the
   *     cluster.
   * @return plan satisfying the placement request.
   */
  default PlacementPlan computePlacement(
      PlacementRequest placementRequest, PlacementContext placementContext)
      throws PlacementException, InterruptedException {
    List<PlacementPlan> placementPlans =
        computePlacements(Collections.singletonList(placementRequest), placementContext);
    if (placementPlans == null || placementPlans.isEmpty()) {
      return null;
    } else {
      return placementPlans.get(0);
    }
  }

  /**
   * Request from plugin code to compute multiple placements. If multiple placements are requested,
   * then the {@link PlacementPlan} computed for each {@link PlacementRequest} will be used to
   * affect the starting state for each subsequent {@link PlacementRequest} in the list. This means
   * that each {@link PlacementRequest} is computed in the context of the previous {@link
   * PlacementRequest}'s already having been implemented. Note this method must be reentrant as a
   * plugin instance may (read will) get multiple such calls in parallel.
   *
   * <p>Configuration is passed upon creation of a new instance of this class by {@link
   * PlacementPluginFactory#createPluginInstance}.
   *
   * @param placementRequests requests for placing new replicas or moving existing replicas on the
   *     cluster.
   * @return plan satisfying all placement requests.
   */
  List<PlacementPlan> computePlacements(
      Collection<PlacementRequest> placementRequests, PlacementContext placementContext)
      throws PlacementException, InterruptedException;

  /**
   * Request from plugin code to compute a balancing of replicas. Note this method must be reentrant
   * as a plugin instance may (read will) get multiple such calls in parallel.
   *
   * <p>Configuration is passed upon creation of a new instance of this class by {@link
   * PlacementPluginFactory#createPluginInstance}.
   *
   * @param balanceRequest request for selecting replicas that should be moved to aid in balancing
   *     the replicas across the desired nodes.
   * @return plan satisfying all extraction requests.
   */
  BalancePlan computeBalancing(BalanceRequest balanceRequest, PlacementContext placementContext)
      throws PlacementException, InterruptedException;

  /**
   * Verify that a collection layout modification doesn't violate constraints on replica placements
   * required by this plugin. Default implementation is a no-op (any modifications are allowed).
   *
   * @param modificationRequest modification request.
   * @param placementContext placement context.
   * @throws PlacementModificationException if the requested modification would violate replica
   *     placement constraints.
   */
  default void verifyAllowedModification(
      ModificationRequest modificationRequest, PlacementContext placementContext)
      throws PlacementException, InterruptedException {}
}
