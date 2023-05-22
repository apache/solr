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

import java.util.Map;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;

/**
 * Allows plugins to create {@link BalancePlan}s telling the Solr layer how to balance replicas
 * following the processing of a {@link BalanceRequest}. The Solr layer can (and will) check that
 * the {@link BalancePlan} conforms to the {@link BalanceRequest} (and if it does not, the requested
 * operation will fail).
 */
public interface BalancePlanFactory {
  /**
   * Creates a {@link BalancePlan} for balancing replicas across the given nodes.
   *
   * <p>This is in support (directly or indirectly) of {@link
   * org.apache.solr.cloud.api.collections.BalanceReplicasCmd}}.
   */
  BalancePlan createBalancePlan(BalanceRequest request, Map<Replica, Node> replicaMovements);
}
