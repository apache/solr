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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.exception.ZeroException;
import org.apache.solr.zero.metadata.MetadataCacheManager;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.apache.solr.zero.util.FileTransferCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the detection and handling of corrupted cores. A corrupted core on Zero store should not
 * continue to be used forever. The current mechanism prevents too many core pull failures from the
 * Zero store. See {@link MetadataCacheManager#recordState}. It also works for IO failures when
 * trying to write the index files locally.
 */
public class CoreCorruptionTest extends ZeroStoreSolrCloudTestCase {

  private ZeroCollectionTestUtil collectionUtil;
  private final int maxCorePullAttempts = 5;

  @After
  public void after() throws Exception {
    shutdownCluster();
  }

  @Before
  public void before() throws Exception {
    System.setProperty(
        ZeroConfig.ZeroSystemProperty.maxFailedCorePullAttempts.getPropertyName(),
        String.valueOf(maxCorePullAttempts));
    System.setProperty(ZeroConfig.ZeroSystemProperty.numCorePullAutoRetries.getPropertyName(), "1");
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullAttemptDelayS.getPropertyName(), "0");
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");

    setupCluster(2);

    collectionUtil = new ZeroCollectionTestUtil(cluster, random());
  }

  /** Tests when the shard metadata file is missing. */
  @Test
  public void testMissingCoreMetadataFile() throws Exception {

    // Inject the BackupRepository to simulate a missing shard metadata.
    TestZeroStoreClient zeroStoreClient = new TestZeroStoreClient(zeroStoreDir.toString());
    zeroStoreClient.simulateMissingShardMetadata = true;
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      runner.getCoreContainer().getZeroStoreManager().replaceZeroStoreClient(zeroStoreClient);
    }
    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();

    // Then loop querying the follower replica to make it try to pull its core from the Zero
    // store.
    // We expect maxPullAttempts and no more. After maxPullAttempts, the pulls are aborted ahead of
    // time, see MetadataCacheManager.recordState().
    Replica follower = collectionUtil.getFollowerReplica();
    for (int i = 0; i < maxCorePullAttempts + 2; i++) {
      ZeroCollectionTestUtil.expectThrowsRemote(
          SolrException.class,
          SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "is not fresh enough because it has never synced with the Zero store",
          () -> query(follower));
    }

    assertEquals(maxCorePullAttempts, zeroStoreClient.shardMetadataExistenceChecks.get());
  }

  /** Tests when the shard metadata pull fails. */
  @Test
  public void testCoreMetadataPullFailure() throws Exception {

    // Inject the BackupRepository to simulate an IO error when pulling the shard metadata.
    TestZeroStoreClient zeroStoreClient = new TestZeroStoreClient(zeroStoreDir.toString());
    zeroStoreClient.simulateIOExceptionReadingShardMetadata = true;
    for (JettySolrRunner runner : cluster.getJettySolrRunners())
      runner.getCoreContainer().getZeroStoreManager().replaceZeroStoreClient(zeroStoreClient);

    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();

    // Then loop querying the follower replica to make it try to pull its core from the Zero
    // store.
    // We expect maxPullAttempts and no more. After maxPullAttempts, the pulls are aborted ahead of
    // time, see MetadataCacheManager.recordState().
    Replica follower = collectionUtil.getFollowerReplica();
    for (int i = 0; i < maxCorePullAttempts + 2; i++) {
      ZeroCollectionTestUtil.expectThrowsRemote(
          SolrException.class,
          SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "is not fresh enough because it has never synced with the Zero store",
          () -> query(follower));
    }

    assertEquals(maxCorePullAttempts, zeroStoreClient.pullCoreAttempts.get());
  }

  /** Tests when an index file pull fails. */
  @Test
  public void testIndexFilePullFailure() throws Exception {

    TestZeroStoreClient zeroStoreClient = new TestZeroStoreClient(zeroStoreDir.toString());
    TestCorePullerFactory corePullerFactory = new TestCorePullerFactory();
    corePullerFactory.simulateIndexFilePullFailure = true;
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      runner.getCoreContainer().getZeroStoreManager().replaceZeroStoreClient(zeroStoreClient);
      // Inject the corePullerFactory to simulate an error when loading the index.
      setupCorePullerFactoryForNode(corePullerFactory, runner);
    }
    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();

    // Then loop querying the follower replica to make it try to pull its core from the Zero
    // store.
    // We expect maxPullAttempts and no more. After maxPullAttempts, the pulls are aborted ahead of
    // time, see MetadataCacheManager.recordState().
    Replica follower = collectionUtil.getFollowerReplica();
    for (int i = 0; i < maxCorePullAttempts + 10; i++) {
      ZeroCollectionTestUtil.expectThrowsRemote(
          SolrException.class,
          SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "is not fresh enough because it has never synced with the Zero store",
          () -> query(follower));
    }
    // TODO often: java.lang.AssertionError: expected:<5> but was:<3>
    assertEquals(maxCorePullAttempts, zeroStoreClient.pullCoreAttempts.get());
  }

  /** Tests when an index file is corrupted and the index cannot be loaded. */
  @Test
  public void testCorruptedIndexFile()
      throws Exception { // TODO seems to fail when executed w other tests

    TestZeroStoreClient zeroStoreClient = new TestZeroStoreClient(zeroStoreDir.toString());
    TestCorePullerFactory corePullerFactory = new TestCorePullerFactory();
    corePullerFactory.simulateIndexFileCorruption.set(Integer.MAX_VALUE);
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      runner
          .getCoreContainer()
          .getZeroStoreManager()
          .replaceZeroStoreClient(
              zeroStoreClient); // Inject the corePullerFactory to simulate an error when loading
      // the index.
      setupCorePullerFactoryForNode(corePullerFactory, runner);
    }
    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();

    // Then loop querying the follower replica to make it try to pull its core from the Zero
    // store.
    // We expect maxPullAttempts and no more. After maxPullAttempts, the pulls are aborted ahead of
    // time, see MetadataCacheManager.recordState().
    Replica follower = collectionUtil.getFollowerReplica();
    for (int i = 0; i < maxCorePullAttempts + 2; i++) {
      ZeroCollectionTestUtil.expectThrowsRemote(
          SolrException.class,
          SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "is not fresh enough because it has never synced with the Zero store",
          () -> query(follower));
      Thread.sleep(100);
    }

    // Verify that we only try to pull a corrupted core twice.
    // (this overrides maxCorePullAttempts, see MetadataCacheManager.recordState())
    assertEquals(2, zeroStoreClient.pullCoreAttempts.get());
  }

  /**
   * Tests when an index file is corrupted on a first core pull and then valid on the second core
   * pull, then the index is fixed and can be loaded.
   */
  @Test
  public void testTransientCorruptionIsFixed() throws Exception {

    TestZeroStoreClient zeroStoreClient = new TestZeroStoreClient(zeroStoreDir.toString());
    TestCorePullerFactory corePullerFactory = new TestCorePullerFactory();
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      runner
          .getCoreContainer()
          .getZeroStoreManager()
          .replaceZeroStoreClient(
              zeroStoreClient); // Inject the corePullerFactory to simulate an error when loading
      // the index.
      setupCorePullerFactoryForNode(corePullerFactory, runner);
    }
    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();

    // Simulate a single transient index file corruption after a first core pull.
    // The core pull is automatically retried and pulls valid index files the second time,
    // so after a few failed queries, we can successfully query the index.
    Replica follower = collectionUtil.getFollowerReplica();
    corePullerFactory.simulateIndexFileCorruption.set(1);
    Exception exception = null;
    for (int i = 0; i < 3; i++) {
      try {
        query(follower);
        exception = null;
        break;
      } catch (Exception e) {
        // Ok to have query failures until the core is successfully pulled (asynchronously).
        exception = e;
        Thread.sleep(500);
      }
    }

    assertNull(exception);
    assertEquals(2, zeroStoreClient.pullCoreAttempts.get());
  }

  /**
   * Tests when an index file is corrupted on a first core pull and then valid after. Verify that
   * the delay between pull attempts is checked before allowing core pull again. Then verify that
   * the index is fixed and can be loaded and queried.
   */
  @Test
  public void testTransientCorruptionIsFixedAfterRetryDelay() throws Exception {

    TestZeroStoreClient zeroStoreClient = new TestZeroStoreClient(zeroStoreDir.toString());
    TestCorePullerFactory corePullerFactory = new TestCorePullerFactory();
    TestPullAttempts testPullAttempts = new TestPullAttempts();
    ZeroConfig config = null;
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      solrRunner
          .getCoreContainer()
          .getZeroStoreManager()
          .replaceZeroStoreClient(
              zeroStoreClient); // Inject the corePullerFactory to simulate an error when loading
      // the index.
      setupCorePullerFactoryForNode(corePullerFactory, solrRunner);
      // Inject a TestPullAttempts to control the time.
      config = solrRunner.getCoreContainer().getZeroStoreManager().getConfig();
      setupTestMetadataCacheManager(
          new MetadataCacheManager(config, solrRunner.getCoreContainer(), () -> testPullAttempts),
          solrRunner);
    }
    collectionUtil.createCollection(2);
    collectionUtil.addDocAndCommit();

    // Simulate a one-time transient index file corruption.
    // The core pull is automatically retried 10 times (ZeroConfig.numCorePullAutoRetries)
    // but they are rejected until some delay elapses
    // (MetadataCacheManager.PullAttempts.inc).
    // After some attempts, mock the PullAttempts time so the delay elapses,
    // and verify that another retry is allowed and fixes the index,
    // so we can successfully query the index.
    Replica follower = collectionUtil.getFollowerReplica();
    corePullerFactory.simulateIndexFileCorruption.set(1);
    Exception exception = null;
    for (int i = 0; i < 10; i++) {
      if (i == 4) {
        // Mock the PullAttempts time so the delay between attempts elapses.
        testPullAttempts.mockedTimeS.set(config.getMaxFailedCorePullAttempts());
      } else {
        // Short time passes, but not enough to reach the delay.
        testPullAttempts.mockedTimeS.incrementAndGet();
      }
      try {
        query(follower);
        exception = null;
        break;
      } catch (Exception e) {
        // Ok to have query failures until the core is successfully pulled (asynchronously).
        exception = e;
        Thread.sleep(500);
      }
    }
    assertNull(exception);
    assertEquals(2, zeroStoreClient.pullCoreAttempts.get());
  }

  private static class TestPullAttempts extends MetadataCacheManager.PullAttempts {

    AtomicLong mockedTimeS = new AtomicLong();

    @Override
    protected long nanoTime() {
      return TimeUnit.SECONDS.toNanos(mockedTimeS.get());
    }
  }

  private void query(Replica replica) throws SolrServerException, IOException {
    try (SolrClient directClient =
        getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName())) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("q", "*:*").set("distrib", "false");
      directClient.query(params);
    }
  }

  private static class TestZeroStoreClient extends ZeroStoreClient {

    volatile boolean simulateIOExceptionReadingShardMetadata;
    final AtomicInteger pullCoreAttempts = new AtomicInteger();
    volatile boolean simulateMissingShardMetadata;
    final AtomicInteger shardMetadataExistenceChecks = new AtomicInteger();

    TestZeroStoreClient(String zeroStoreDir) {
      super(new LocalFileSystemRepository(), new SolrMetricManager(), 30, 10);
      NamedList<String> args = new NamedList<>();
      args.add(CoreAdminParams.BACKUP_LOCATION, zeroStoreDir);
      zeroRepository.init(args);
    }

    @Override
    public ZeroStoreShardMetadata pullShardMetadata(ZeroFile zeroShardMetadataFile)
        throws ZeroException {
      pullCoreAttempts.incrementAndGet();
      if (simulateIOExceptionReadingShardMetadata) {
        throw new ZeroException(new IOException("Simulated IO exception pulling shard metadata"));
      }
      return super.pullShardMetadata(zeroShardMetadataFile);
    }

    @Override
    public boolean shardMetadataExists(ZeroFile zeroShardMetadataFile) throws ZeroException {
      shardMetadataExistenceChecks.incrementAndGet();
      return !simulateMissingShardMetadata && super.shardMetadataExists(zeroShardMetadataFile);
    }
  }

  private static class TestCorePullerFactory implements ZeroStoreManager.CorePullerFactory {

    volatile boolean simulateIndexFilePullFailure;
    final AtomicInteger simulateIndexFileCorruption = new AtomicInteger();

    @Override
    public CorePuller createPuller(
        SolrCore solrCore,
        ZeroStoreClient zeroStoreClient,
        MetadataCacheManager metadataCacheManager,
        ZeroMetadataController metadataController,
        int maxFailedCorePullAttempts) {
      return new CorePuller(
          solrCore,
          zeroStoreClient,
          metadataCacheManager,
          metadataController,
          maxFailedCorePullAttempts,
          null) {

        @Override
        protected void pullZeroFiles(
            Directory tempIndexDir,
            Collection<ZeroFile.WithLocal> filesToDownload,
            FileTransferCounter counter)
            throws InterruptedException, ExecutionException {
          if (simulateIndexFilePullFailure) {
            throw new RuntimeException("Simulated index file pull failure");
          }
          super.pullZeroFiles(tempIndexDir, filesToDownload, counter);
        }

        @Override
        protected void openIndexWriter(boolean createNewIndexDir, boolean coreSwitchedToNewIndexDir)
            throws Exception {
          // If random().nextBoolean() returns false, the simulated corruption will be thrown in
          // openSearcher().
          if (random().nextBoolean() && simulateIndexFileCorruption.decrementAndGet() >= 0) {
            if (!createNewIndexDir) {
              try {
                // We must call SolrCoreState.openIndexWriter() to release the write lock.
                // Pass a null solrCore to provoke a NPE, but with this call we unlock
                // the write lock.
                solrCore.getUpdateHandler().getSolrCoreState().openIndexWriter(null);
              } catch (NullPointerException ignored) {
                // Catch and ignore the expected NPE.
              }
            }
            if (random().nextBoolean()) {
              throw new CorruptIndexException("Simulated index corruption", "Test");
            } else {
              throw new IOException("Simulated index corruption");
            }
          }
          super.openIndexWriter(createNewIndexDir, coreSwitchedToNewIndexDir);
        }

        @Override
        public void openSearcher(boolean waitForSearcher) throws Exception {
          if (simulateIndexFileCorruption.decrementAndGet() >= 0) {
            if (random().nextBoolean()) {
              throw new SolrException(
                  SolrException.ErrorCode.SERVER_ERROR, "Simulated index corruption");
            } else {
              throw new Exception("Simulated index corruption");
            }
          }
          super.openSearcher(waitForSearcher);
        }
      };
    }
  }
}
