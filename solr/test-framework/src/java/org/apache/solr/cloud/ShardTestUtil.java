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
package org.apache.solr.cloud;

import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;

public class ShardTestUtil {

  public static void setSliceState(
      MiniSolrCloudCluster cluster, String collection, String slice, Slice.State state)
      throws Exception {

    MapWriter m =
        ew ->
            ew.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower())
                .put(slice, state.toString())
                .put(ZkStateReader.COLLECTION_PROP, collection);
    final Overseer overseer = cluster.getOpenOverseer();
    if (overseer.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
      overseer
          .getDistributedClusterStateUpdater()
          .doSingleStateUpdate(
              DistributedClusterStateUpdater.MutatingCommand.SliceUpdateShardState,
              new ZkNodeProps(m),
              cluster.getOpenOverseer().getSolrCloudManager(),
              cluster.getOpenOverseer().getZkStateReader());
    } else {
      DistributedQueue inQueue =
          cluster
              .getJettySolrRunner(0)
              .getCoreContainer()
              .getZkController()
              .getOverseer()
              .getStateUpdateQueue();
      inQueue.offer(m);
    }
  }
}
