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

import java.io.IOException;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.metrics.SolrMetricsContext;

/**
 * An {@link OverseerTaskProcessor} that handles: 1) collection-related Overseer messages 2)
 * configset-related Overseer messages
 */
public class OverseerCollectionConfigSetProcessor extends OverseerTaskProcessor {

  private static final String CONFIGSETS_ACTION_PREFIX = "configsets:";

  public OverseerCollectionConfigSetProcessor(
      ZkStateReader zkStateReader,
      String myId,
      final HttpShardHandler shardHandler,
      String adminPath,
      Stats stats,
      Overseer overseer,
      OverseerNodePrioritizer overseerNodePrioritizer,
      SolrMetricsContext solrMetricsContext) {
    this(
        zkStateReader,
        myId,
        (HttpShardHandlerFactory) shardHandler.getShardHandlerFactory(),
        adminPath,
        stats,
        overseer,
        overseerNodePrioritizer,
        overseer.getCollectionQueue(zkStateReader.getZkClient(), stats),
        Overseer.getRunningMap(zkStateReader.getZkClient()),
        Overseer.getCompletedMap(zkStateReader.getZkClient()),
        Overseer.getFailureMap(zkStateReader.getZkClient()),
        solrMetricsContext);
  }

  protected OverseerCollectionConfigSetProcessor(
      ZkStateReader zkStateReader,
      String myId,
      final HttpShardHandlerFactory shardHandlerFactory,
      String adminPath,
      Stats stats,
      Overseer overseer,
      OverseerNodePrioritizer overseerNodePrioritizer,
      OverseerTaskQueue workQueue,
      DistributedMap runningMap,
      DistributedMap completedMap,
      DistributedMap failureMap,
      SolrMetricsContext solrMetricsContext) {
    super(
        zkStateReader,
        myId,
        stats,
        getOverseerMessageHandlerSelector(
            zkStateReader,
            myId,
            shardHandlerFactory,
            adminPath,
            stats,
            overseer,
            overseerNodePrioritizer),
        overseerNodePrioritizer,
        workQueue,
        runningMap,
        completedMap,
        failureMap,
        solrMetricsContext);
  }

  private static OverseerMessageHandlerSelector getOverseerMessageHandlerSelector(
      ZkStateReader zkStateReader,
      String myId,
      final HttpShardHandlerFactory shardHandlerFactory,
      String adminPath,
      Stats stats,
      Overseer overseer,
      OverseerNodePrioritizer overseerNodePrioritizer) {
    final OverseerCollectionMessageHandler collMessageHandler =
        new OverseerCollectionMessageHandler(
            zkStateReader,
            myId,
            shardHandlerFactory,
            adminPath,
            stats,
            overseer,
            overseerNodePrioritizer);
    return new OverseerMessageHandlerSelector() {
      @Override
      public void close() throws IOException {
        IOUtils.closeQuietly(collMessageHandler);
      }

      @Override
      public OverseerMessageHandler selectOverseerMessageHandler(String operation) {
        if (operation.startsWith(CONFIGSETS_ACTION_PREFIX)) {
          return null;
        }
        return collMessageHandler;
      }
    };
  }
}
