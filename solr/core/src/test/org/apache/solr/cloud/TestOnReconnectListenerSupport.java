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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class TestOnReconnectListenerSupport extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "false");
    configureCluster(1).addConfig("conf1", configset("cloud-managed")).configure();
  }

  @Test
  public void test() throws Exception {
    String testCollectionName = "c8n_onreconnect_1x1";
    CollectionAdminRequest.createCollection(testCollectionName, "conf1", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(testCollectionName, 1, 1);

    Replica leader =
        getCollectionState(testCollectionName).replicaStream().findFirst().orElseThrow();
    CoreContainer cores = cluster.getJettySolrRunner(0).getCoreContainer();
    ZkController zkController = cores.getZkController();
    assertNotNull("ZkController is null", zkController);

    String leaderCoreName = leader.getCoreName();
    String leaderCoreId;
    try (SolrCore leaderCore = cores.getCore(leaderCoreName)) {
      assertNotNull("SolrCore for " + leaderCoreName + " not found!", leaderCore);
      leaderCoreId = leaderCore.getName() + ":" + leaderCore.getStartNanoTime();
    }

    // verify the ZkIndexSchemaReader is a registered OnReconnect listener
    assertNotNull(
        "ZkIndexSchemaReader for core "
            + leaderCoreName
            + " not registered as an OnReconnect listener and should be",
        findSchemaReaderListener(zkController, leaderCoreId));

    // reload the collection; the reloaded core should be registered as an OnReconnect listener
    // and the old core should not be
    CollectionAdminRequest.reloadCollection(testCollectionName).process(cluster.getSolrClient());

    String reloadedLeaderCoreId;
    try (SolrCore leaderCore = cores.getCore(leaderCoreName)) {
      reloadedLeaderCoreId = leaderCore.getName() + ":" + leaderCore.getStartNanoTime();
    }

    // they shouldn't be equal after reload
    assertNotEquals(leaderCoreId, reloadedLeaderCoreId);

    assertNull(
        "Previous core "
            + leaderCoreId
            + " should no longer be a registered OnReconnect listener! Current listeners: "
            + zkController.getCurrentOnReconnectListeners(),
        findSchemaReaderListener(zkController, leaderCoreId));
    assertNotNull(
        "ZkIndexSchemaReader for core "
            + reloadedLeaderCoreId
            + " not registered as an OnReconnect listener and should be",
        findSchemaReaderListener(zkController, reloadedLeaderCoreId));

    // deleting the collection should unregister the listener once the core closes
    CollectionAdminRequest.deleteCollection(testCollectionName).process(cluster.getSolrClient());

    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "Core "
            + reloadedLeaderCoreId
            + " should no longer be a registered OnReconnect listener after collection delete!",
        () -> findSchemaReaderListener(zkController, reloadedLeaderCoreId) == null);
  }

  /** Returns the registered {@link ZkIndexSchemaReader} listener for the given core id, if any. */
  private static ZkIndexSchemaReader findSchemaReaderListener(
      ZkController zkController, String coreId) {
    Set<OnReconnect> listeners = zkController.getCurrentOnReconnectListeners();
    assertNotNull("ZkController returned null OnReconnect listeners", listeners);
    for (OnReconnect listener : listeners) {
      if (listener instanceof ZkIndexSchemaReader reader
          && coreId.equals(reader.getUniqueCoreId())) {
        return reader;
      }
    }
    return null;
  }
}
