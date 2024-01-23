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
package org.apache.solr.zero.process;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.MiniSolrCloudCluster.Builder;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.metadata.LocalCoreMetadata;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.MetadataComparisonResult;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class for SolrCloud tests with a few additional utilities for testing with a Zero store
 *
 * <p>Derived tests should call {@link #configureCluster(int)} in a {@code BeforeClass} static
 * method or {@code Before} setUp method. This configures and starts a {@link MiniSolrCloudCluster},
 * available via the {@code cluster} variable. Cluster shutdown is handled automatically if using
 * {@code BeforeClass}.
 *
 * <pre>
 *   <code>
 *   {@literal @}BeforeClass
 *   public static void setupCluster() {
 *     configureCluster(NUM_NODES)
 *        .addConfig("configname", pathToConfig)
 *        .configure();
 *   }
 *   </code>
 * </pre>
 */
public abstract class ZeroStoreSolrCloudTestCase extends SolrCloudTestCase {
  public static String DEFAULT_ZERO_STORE_DIR_NAME = "LocalZeroStore/";

  public static Path zeroDir;
  public static Path zeroStoreDir;

  @BeforeClass
  public static void setupZeroDirectory() {
    assumeWorkingMockito();
    zeroDir = createTempDir("tempDir");
    zeroStoreDir = zeroDir.resolve(DEFAULT_ZERO_STORE_DIR_NAME);
    zeroStoreDir.toFile().mkdirs();
  }

  @AfterClass
  public static void cleanupZeroDirectory() throws Exception {
    if (zeroDir != null) {
      FileUtils.cleanDirectory(zeroDir.toFile());
    }
    // clean up any properties used by Zero store tests
    System.clearProperty(ZeroConfig.ZeroSystemProperty.ZeroStoreEnabled.getPropertyName());
  }

  public static void setupZeroCollectionWithShardNames(
      String collectionName, int numReplicas, String shardNames) throws Exception {
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollectionWithImplicitRouter(
                collectionName, "conf", shardNames, 0)
            .setZeroIndex(true)
            .setZeroReplicas(numReplicas);
    create.process(cluster.getSolrClient());
    int numShards = shardNames.split(",").length;

    // Verify that collection was created
    waitForState(
        "Timed-out wait for collection to be created",
        collectionName,
        clusterShape(numShards, numShards * numReplicas));
  }

  /**
   * Spin up a {@link MiniSolrCloudCluster} with Zero store enabled and the local FS as the Zero
   * storage provider
   */
  public static void setupCluster(int nodes) throws Exception {
    configureClusterZeroEnabled(nodes).configure();
  }

  /**
   * Spin up a {@link MiniSolrCloudCluster} with Zero store enabled and the local FS as the Zero
   * store provider.
   */
  public static void setupCluster(int nodes, String solrXml) throws Exception {
    configureClusterZeroEnabled(nodes).withSolrXml(solrXml).configure();
  }

  private static Builder configureClusterZeroEnabled(int nodes) {
    System.setProperty(ZeroConfig.ZeroSystemProperty.ZeroStoreEnabled.getPropertyName(), "true");
    String solrXml =
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replaceFirst(
            "<solr>",
            "<solr>"
                + "  <zero>"
                + "    <repositories>"
                + "      <repository\n"
                + "        name=\"local\""
                + "        class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\""
                + "        default=\"true\">"
                + "        <str name=\"location\">"
                + zeroStoreDir
                + "</str>"
                + "      </repository>"
                + "    </repositories>"
                + "  </zero>");

    return configureCluster(nodes)
        .withSolrXml(solrXml)
        .addConfig("conf", configset("cloud-minimal-zero"));
  }

  /** Spin up a {@link MiniSolrCloudCluster} with Zero store disabled */
  public static void setupClusterZeroDisable(int nodes) throws Exception {
    // make sure the feature is explicitly disabled in case programmer error results in the property
    // remaining enabled
    System.setProperty(ZeroConfig.ZeroSystemProperty.ZeroStoreEnabled.getPropertyName(), "false");
    configureCluster(nodes).addConfig("conf", configset("cloud-minimal")).configure();
  }

  /**
   * Once this method is called, background async pulls (triggered by queries) are no longer done.
   */
  protected static void stopAsyncCorePulling(JettySolrRunner solrRunner) {
    ZeroStoreManager manager = solrRunner.getCoreContainer().getZeroStoreManager();
    manager.stopBackgroundCorePulling();
  }

  /** Configures the Solr process with the given {@link MetadataCacheManager} */
  protected static void setupTestMetadataCacheManager(
      MetadataCacheManager metadataCacheManager, JettySolrRunner solrRunner) {
    ZeroStoreManager manager = solrRunner.getCoreContainer().getZeroStoreManager();
    manager.replaceMetadataCacheManager(metadataCacheManager);
  }

  /** Configures the Solr process with the given {@link DeleteProcessor} */
  protected static void setupDeleteProcessorsForNode(
      DeleteProcessor deleteProcessor,
      DeleteProcessor overseerDeleteProcessor,
      JettySolrRunner solrRunner) {
    ZeroStoreManager manager = solrRunner.getCoreContainer().getZeroStoreManager();
    manager.replaceDeleteProcessors(deleteProcessor, overseerDeleteProcessor);
  }

  /**
   * Configures the Solr process with a new {@link ZeroStoreManager.CorePullerFactory} for {@link
   * CorePuller}
   */
  protected static void setupCorePullerFactoryForNode(
      ZeroStoreManager.CorePullerFactory corePullerFactory, JettySolrRunner solrRunner) {
    ZeroStoreManager manager = solrRunner.getCoreContainer().getZeroStoreManager();
    manager.replaceCorePullerFactory(corePullerFactory);
  }

  /** Configures the Solr process with the given {@link ZeroMetadataController} */
  protected static void setupZeroShardMetadataControllerForNode(
      ZeroMetadataController metadataController, JettySolrRunner solrRunner) {
    ZeroStoreManager manager = solrRunner.getCoreContainer().getZeroStoreManager();
    manager.replaceZeroShardMetadataController(metadataController);
  }

  /** Configures the Solr process with the given {@link ZeroStoreClient} */
  protected static void setupZeroStoreClientForNode(
      ZeroStoreClient zeroStoreClient, JettySolrRunner solrRunner) {
    solrRunner.getCoreContainer().getZeroStoreManager().replaceZeroStoreClient(zeroStoreClient);
  }

  /** Return a new ZeroStoreClient that writes to a local file system repository */
  protected static ZeroStoreClient setupLocalZeroStoreClient(ZeroConfig config) {
    NamedList<String> args = new NamedList<>();
    args.add("location", zeroStoreDir.toString());
    LocalFileSystemRepository repo = new LocalFileSystemRepository();
    repo.init(args);
    return new ZeroStoreClient(
        repo,
        new SolrMetricManager(),
        config.getNumFilePusherThreads(),
        config.getNumFilePullerThreads());
  }

  /**
   * Injects into a MiniSolrCloudCluster per core latch counted down on pull end.
   *
   * @return an empty map from core name to latch to be populated by the caller. If an entry exists
   *     for a core when a pull ends with {@link CorePullStatus#SUCCESS} or {@link
   *     CorePullStatus#NOT_NEEDED}, the corresponding latch will be {@link
   *     CountDownLatch#countDown()}
   */
  protected static Map<String, CountDownLatch> configureTestZeroProcessForNode(
      JettySolrRunner runner) {
    final Map<String, CountDownLatch> pullLatches = new HashMap<>();

    ZeroStoreManager.CorePullerFactory cpf =
        (solrCore,
            zeroStoreClient,
            metadataCacheManager,
            metadataController,
            maxFailedCorePullAttempts) ->
            new CorePuller(
                solrCore,
                zeroStoreClient,
                metadataCacheManager,
                metadataController,
                maxFailedCorePullAttempts,
                (corePuller, corePullStatus) -> {
                  CountDownLatch latch = pullLatches.get(corePuller.getDedupeKey());
                  if (latch != null
                      && (corePullStatus == CorePullStatus.SUCCESS
                          || corePullStatus == CorePullStatus.NOT_NEEDED)) {
                    latch.countDown();
                  }
                });

    setupCorePullerFactoryForNode(cpf, runner);
    return pullLatches;
  }

  protected static void primingPullQuery(SolrClient replicaClient, SolrParams params)
      throws Exception {
    primingPullQuery(replicaClient, null, params);
  }

  /**
   * If a ZERO replica is queried and it has never synced with the Zero store, it will fail the
   * query with a specific error. Queries that are meant to trigger a pull from the Zero store are
   * likely to hit that error (if used in the context of first pull). Since the intention is to
   * trigger the pull (irrespective of the specific error) therefore this method helps with eating
   * that error (if that happens)
   */
  protected static void primingPullQuery(SolrClient client, String collection, SolrParams params)
      throws Exception {
    try {
      client.query(collection, params);
    } catch (Exception ex) {
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw));
      if (!sw.toString()
          .contains("is not fresh enough because it has never synced with the Zero store")) {
        throw ex;
      }
    }
  }

  @SuppressWarnings("try")
  protected static ZeroStoreShardMetadata doPush(
      SolrCore core,
      String collectionName,
      String shardName,
      DeleteProcessor deleteProcessor,
      ZeroStoreClient zeroStoreClient)
      throws Exception {

    // initialize metadata info to match initial zk version
    MetadataCacheManager metadataCacheManager =
        new MetadataCacheManager(new ZeroConfig(), core.getCoreContainer()) {
          @Override
          protected String getCollectionName(SolrCore solrCore) {
            return collectionName;
          }

          @Override
          protected String getShardName(SolrCore solrCore) {
            return shardName;
          }
        };

    ZeroMetadataController metadataController = new ZeroMetadataController(null);

    // since we're pushing outside the designated path, just acquire a lock to allow the
    // updateCoreMetadata to go through
    try (AutoCloseable ignore =
        metadataCacheManager
            .getOrCreateCoreMetadata(core.getName())
            .getZeroAccessLocks()
            .acquirePullLock(5)) {
      metadataCacheManager.updateCoreMetadata(
          core.getName(),
          new ZeroMetadataVersion(ZeroMetadataController.METADATA_NODE_DEFAULT_VALUE, 0),
          new ZeroStoreShardMetadata(),
          false);
    }

    // empty shardMetadata means we should push everything we have locally
    ZeroStoreShardMetadata shardMetadata = new ZeroStoreShardMetadata();

    // build the required metadata
    LocalCoreMetadata localCoreMetadata =
        new LocalCoreMetadata(core) {
          @Override
          protected String getCollectionName() {
            return collectionName;
          }

          @Override
          protected String getShardName() {
            return shardName;
          }

          @Override
          protected String getCoreName() {
            return coreName;
          }
        };

    localCoreMetadata.readMetadata(true, false);
    MetadataComparisonResult metadataComparisonResult =
        metadataController.diffMetadataForPush(
            localCoreMetadata, shardMetadata, ZeroStoreShardMetadata.generateMetadataSuffix());
    // the returned ZeroStoreShardMetadata is what is pushed to Zero store so we should verify
    // the push was made with the correct data
    CorePusher corePusher =
        new CorePusher(
            core, zeroStoreClient, deleteProcessor, metadataCacheManager, metadataController) {
          @Override
          public void enqueueForHardDelete(ZeroStoreShardMetadata shardMetadata) {}

          @Override
          protected String getCoreName() {
            return coreName;
          }

          @Override
          protected String getShardName() {
            return shardName;
          }

          @Override
          protected String getCollectionName() {
            return collectionName;
          }
        };
    return corePusher.pushFilesToZeroStore(metadataComparisonResult);
  }
}
