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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.api.collections.AdminCmdContext;
import org.apache.solr.cloud.api.collections.CollectionCommandContext;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.cloud.api.collections.DistributedCollectionCommandContext;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.BalanceRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.impl.DelegatingPlacementPluginFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateCollectionCleanupTest extends SolrCloudTestCase {

  protected static final String CLOUD_SOLR_XML_WITH_10S_CREATE_COLL_WAIT =
      "<solr>\n"
          + "\n"
          + "  <str name=\"shareSchema\">${shareSchema:false}</str>\n"
          + "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n"
          + "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n"
          + "\n"
          + "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n"
          + "    <str name=\"urlScheme\">${urlScheme:}</str>\n"
          + "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n"
          + "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n"
          + "  </shardHandlerFactory>\n"
          + "\n"
          + "  <solrcloud>\n"
          + "    <str name=\"host\">127.0.0.1</str>\n"
          + "    <int name=\"hostPort\">${hostPort:8983}</int>\n"
          + "    <int name=\"zkClientTimeout\">${solr.zookeeper.client.timeout:30000}</int>\n"
          + "    <int name=\"leaderVoteWait\">10000</int>\n"
          + "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n"
          + "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n"
          + "    <int name=\"createCollectionWaitTimeTillActive\">${createCollectionWaitTimeTillActive:10}</int>\n"
          + "  </solrcloud>\n"
          + "  \n"
          + "</solr>\n";

  private static final String PLACEMENT_FAILURE = "simulated placement failure";

  @BeforeClass
  public static void createCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(CLOUD_SOLR_XML_WITH_10S_CREATE_COLL_WAIT)
        .configure();
  }

  @After
  public void restoreDefaultPlacement() {
    setPlacementPluginFactory(null);
  }

  @Test
  public void testCreateCollectionCleanup() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "foo";
    assertThat(CollectionAdminRequest.listCollections(cloudClient), not(hasItem(collectionName)));
    // Create a collection that would fail
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1);

    Properties properties = new Properties();
    Path tmpDir = createTempDir();
    tmpDir = tmpDir.resolve("foo");
    Files.createFile(tmpDir);
    properties.put(CoreAdminParams.DATA_DIR, tmpDir.toString());
    create.setProperties(properties);
    expectThrows(
        RemoteSolrException.class,
        () -> {
          create.process(cloudClient);
        });

    // Confirm using LIST that the collection does not exist
    assertThat(
        "Failed collection is still in the clusterstate: "
            + cluster.getSolrClient().getClusterState().getCollectionOrNull(collectionName),
        CollectionAdminRequest.listCollections(cloudClient),
        not(hasItem(collectionName)));
  }

  @Test
  public void testAsyncCreateCollectionCleanup() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "foo2";
    assertThat(CollectionAdminRequest.listCollections(cloudClient), not(hasItem(collectionName)));

    // Create a collection that would fail
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1)
            .setPerReplicaState(random().nextBoolean());

    Properties properties = new Properties();
    Path tmpDir = createTempDir();
    tmpDir = tmpDir.resolve("foo");
    Files.createFile(tmpDir);
    properties.put(CoreAdminParams.DATA_DIR, tmpDir.toString());
    create.setProperties(properties);
    create.setAsyncId("testAsyncCreateCollectionCleanup");
    create.process(cloudClient);
    RequestStatusState state =
        AbstractFullDistribZkTestBase.getRequestStateAfterCompletion(
            "testAsyncCreateCollectionCleanup", 30, cloudClient);
    assertThat(state.getKey(), is("failed"));

    // Confirm using LIST that the collection does not exist
    assertThat(
        "Failed collection is still in the clusterstate: "
            + cluster.getSolrClient().getClusterState().getCollectionOrNull(collectionName),
        CollectionAdminRequest.listCollections(cloudClient),
        not(hasItem(collectionName)));
  }

  /** A placement plugin failure that is not a {@link PlacementException}, such as a plugin bug. */
  @Test
  public void testCleanupAfterUnexpectedPlacementFailure() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "foo3";
    setPlacementPluginFactory(new FailingPlacementFactory(false));

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1)
            .setPerReplicaState(random().nextBoolean());
    RemoteSolrException e =
        expectThrows(RemoteSolrException.class, () -> create.process(cloudClient));
    assertThat(e.getMessage(), containsString(PLACEMENT_FAILURE));
    assertNoCollection(collectionName);

    // nothing is left that would get in the way of creating the collection again
    setPlacementPluginFactory(null);
    CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1).process(cloudClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);
    CollectionAdminRequest.deleteCollection(collectionName).process(cloudClient);
  }

  /** A placement plugin that rejects the request the way plugins are expected to. */
  @Test
  public void testCleanupAfterPlacementException() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "foo4";
    setPlacementPluginFactory(new FailingPlacementFactory(true));

    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1)
            .setPerReplicaState(random().nextBoolean());
    RemoteSolrException e =
        expectThrows(RemoteSolrException.class, () -> create.process(cloudClient));
    assertThat(e.getMessage(), containsString(PLACEMENT_FAILURE));
    assertNoCollection(collectionName);
  }

  /**
   * A create that names a collection which already exists must fail with BAD_REQUEST and must not
   * delete that collection. The command is invoked directly with a cluster state that does not
   * contain the collection, the view a node hands to the command when its local state lags behind
   * ZooKeeper. The up-front check passes on that view, so only the state.json check in ZooKeeper
   * keeps the create from going ahead and, on a later failure, deleting a collection it did not
   * create.
   */
  @Test
  public void testCreateDoesNotDeleteExistingCollectionOnStaleView() throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    String collectionName = "existingColl";
    CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1).process(cloudClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);
    try {
      String collectionPath = DocCollection.getCollectionPath(collectionName);
      byte[] stateBefore = zkClient().getData(collectionPath, null, null);
      assertNotNull(stateBefore);

      // the stale view: same live nodes, but the collection is not in it
      ClusterState currentState = cloudClient.getClusterState();
      Map<String, ClusterState.CollectionRef> otherCollections = new HashMap<>();
      for (String name : currentState.getCollectionNames()) {
        if (!name.equals(collectionName)) {
          otherCollections.put(name, currentState.getCollectionRef(name));
        }
      }
      ClusterState staleState = new ClusterState(otherCollections, currentState.getLiveNodes());

      CoreContainer coreContainer = cluster.getJettySolrRunners().get(0).getCoreContainer();
      ExecutorService executor =
          ExecutorUtil.newMDCAwareSingleThreadExecutor(
              new SolrNamedThreadFactory("createCollectionCleanupTest"));
      try {
        CollectionCommandContext ccc =
            new DistributedCollectionCommandContext(coreContainer, executor);
        AdminCmdContext adminCmdContext =
            new AdminCmdContext(CollectionParams.CollectionAction.CREATE)
                .withClusterState(staleState);
        ZkNodeProps message =
            new ZkNodeProps(
                "name",
                collectionName,
                CollectionAdminParams.COLL_CONF,
                "conf1",
                "numShards",
                "1",
                "nrtReplicas",
                "1",
                DocCollection.CollectionStateProps.PER_REPLICA_STATE,
                "false");
        SolrException e =
            expectThrows(
                SolrException.class,
                () ->
                    new CreateCollectionCmd(ccc).call(adminCmdContext, message, new NamedList<>()));

        // the existing collection is untouched: same state.json, still listed
        assertArrayEquals(stateBefore, zkClient().getData(collectionPath, null, null));
        assertThat(CollectionAdminRequest.listCollections(cloudClient), hasItem(collectionName));
        assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
        assertThat(e.getMessage(), containsString("collection already exists: " + collectionName));
      } finally {
        executor.shutdown();
      }
    } finally {
      CollectionAdminRequest.deleteCollection(collectionName).process(cloudClient);
    }
  }

  private static void assertNoCollection(String collectionName) throws Exception {
    assertThat(
        "Failed collection is still in the clusterstate: "
            + cluster.getSolrClient().getClusterState().getCollectionOrNull(collectionName),
        CollectionAdminRequest.listCollections(cluster.getSolrClient()),
        not(hasItem(collectionName)));
    assertFalse(
        "Failed collection still has a node in ZooKeeper",
        zkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName));
  }

  /** Sets the placement plugin factory of the node; {@code null} restores the default. */
  private static void setPlacementPluginFactory(PlacementPluginFactory<?> factory) {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      ((DelegatingPlacementPluginFactory) jetty.getCoreContainer().getPlacementPluginFactory())
          .setDelegate(factory);
    }
  }

  private static class FailingPlacementFactory
      implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {
    private final boolean placementException;

    FailingPlacementFactory(boolean placementException) {
      this.placementException = placementException;
    }

    @Override
    public PlacementPlugin createPluginInstance() {
      return new PlacementPlugin() {
        @Override
        public List<PlacementPlan> computePlacements(
            Collection<PlacementRequest> placementRequests, PlacementContext placementContext)
            throws PlacementException {
          if (placementException) {
            throw new PlacementException(PLACEMENT_FAILURE);
          }
          throw new IllegalStateException(PLACEMENT_FAILURE);
        }

        @Override
        public BalancePlan computeBalancing(
            BalanceRequest balanceRequest, PlacementContext placementContext) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
