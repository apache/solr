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

package org.apache.solr.cloud.api.collections;

import java.util.concurrent.ExecutorService;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandler;

public class DistributedCollectionCommandContext implements CollectionCommandContext {
  private final CoreContainer coreContainer;
  private final DistributedClusterStateUpdater getDistributedClusterStateUpdater;
  private final ExecutorService executorService;

  private final SolrCloudManager solrCloudManager;
  private final ZkStateReader zkStateReader;

  public DistributedCollectionCommandContext(
      CoreContainer coreContainer, ExecutorService executorService) {
    this.coreContainer = coreContainer;
    this.getDistributedClusterStateUpdater =
        new DistributedClusterStateUpdater(
            coreContainer.getConfig().getCloudConfig().getDistributedClusterStateUpdates());
    ;
    this.executorService = executorService;

    solrCloudManager = this.coreContainer.getZkController().getSolrCloudManager();
    zkStateReader = this.coreContainer.getZkController().getZkStateReader();
  }

  @Override
  public boolean isDistributedCollectionAPI() {
    return true;
  }

  @Override
  public ShardHandler newShardHandler() {
    // This method builds a new shard handler! A given shard handler can't be reused because it can
    // only serve a single thread.
    return this.coreContainer.getShardHandlerFactory().getShardHandler();
  }

  @Override
  public SolrCloudManager getSolrCloudManager() {
    return solrCloudManager;
  }

  @Override
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  @Override
  public DistributedClusterStateUpdater getDistributedClusterStateUpdater() {
    return this.getDistributedClusterStateUpdater;
  }

  @Override
  public SolrCloseable getCloseableToLatchOn() {
    // Debatable: SolrCloudManager instance is tracked for closing to interrupt some command
    // execution. Could very well monitor something else.
    return getSolrCloudManager();
  }

  @Override
  public ExecutorService getExecutorService() {
    return this.executorService;
  }
}
