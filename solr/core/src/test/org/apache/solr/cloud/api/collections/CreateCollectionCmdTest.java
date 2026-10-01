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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.NUM_SHARDS;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.Watcher;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateCollectionCmdTest extends SolrTestCase {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testAssignmentFailureCleansUpCollectionZkNode() throws Exception {
    String collectionName = "missing-collection";
    String nodeName = "node1:8983_solr";
    ClusterState clusterState = new ClusterState(Set.of(nodeName), Map.of());

    SolrCloudManager cloudManager = mock(SolrCloudManager.class);
    ClusterStateProvider stateProvider = mock(ClusterStateProvider.class);
    when(stateProvider.getLiveNodes()).thenReturn(Set.of(nodeName));
    when(cloudManager.getClusterStateProvider()).thenReturn(stateProvider);
    when(cloudManager.getClusterState()).thenReturn(clusterState);

    DistribStateManager distribStateManager = mock(DistribStateManager.class);
    when(distribStateManager.listData(anyString())).thenThrow(new NoSuchElementException());
    when(cloudManager.getDistribStateManager()).thenReturn(distribStateManager);

    ConfigSetService configSetService = mock(ConfigSetService.class);
    when(configSetService.listConfigs()).thenReturn(List.of("_default"));
    when(configSetService.checkConfigExists("_default")).thenReturn(true);
    PlacementPluginFactory<?> placementPluginFactory = mock(PlacementPluginFactory.class);
    when(placementPluginFactory.createPluginInstance()).thenReturn(mock(PlacementPlugin.class));
    CoreContainer coreContainer = mock(CoreContainer.class);
    when(coreContainer.getConfigSetService()).thenReturn(configSetService);
    when(coreContainer.getPlacementPluginFactory()).thenReturn(placementPluginFactory);

    SolrZkClient zkClient = mock(SolrZkClient.class);
    when(zkClient.getChildren(anyString(), isNull(Watcher.class))).thenReturn(List.of());
    when(zkClient.exists(anyString())).thenReturn(true);
    ZkStateReader zkStateReader = mock(ZkStateReader.class);
    when(zkStateReader.getAliases()).thenReturn(Aliases.EMPTY);
    when(zkStateReader.getClusterState()).thenReturn(clusterState);
    when(zkStateReader.getZkClient()).thenReturn(zkClient);

    DistributedClusterStateUpdater stateUpdater = mock(DistributedClusterStateUpdater.class);
    when(stateUpdater.isDistributedStateUpdate()).thenReturn(false);

    CollectionCommandContext ccc = mock(CollectionCommandContext.class);
    when(ccc.getCoreContainer()).thenReturn(coreContainer);
    when(ccc.getDistributedClusterStateUpdater()).thenReturn(stateUpdater);
    when(ccc.getSolrCloudManager()).thenReturn(cloudManager);
    when(ccc.getZkStateReader()).thenReturn(zkStateReader);

    ZkNodeProps message =
        new ZkNodeProps(
            Map.of(
                NAME,
                collectionName,
                COLL_CONF,
                "_default",
                NUM_SHARDS,
                1,
                REPLICATION_FACTOR,
                1,
                CREATE_NODE_SET_PARAM,
                nodeName));
    AdminCmdContext adminCmdContext =
        new AdminCmdContext(CollectionParams.CollectionAction.CREATE)
            .withClusterState(clusterState);

    SolrException exception =
        expectThrows(
            SolrException.class,
            () ->
                new CreateCollectionCmd(ccc)
                    .call(adminCmdContext, message, new NamedList<>()));

    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, exception.code());
    assertTrue(exception.getMessage().contains(collectionName));
    verify(zkClient).clean(eq(DocCollection.getCollectionPathRoot(collectionName)));
  }
}
