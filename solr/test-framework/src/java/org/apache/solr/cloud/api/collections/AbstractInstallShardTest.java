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

import static org.apache.solr.client.solrj.request.CollectionAdminRequest.deleteCollection;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.admin.api.InstallShardData;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for testing the "Install Shard API" with various backup repositories.
 *
 * <p>Subclasses are expected to bootstrap a Solr cluster with a single configured backup
 * repository. This base-class will populate that backup repository all data necessary for these
 * tests.
 *
 * @see InstallShardData
 */
public abstract class AbstractInstallShardTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final String BACKUP_REPO_NAME = "trackingBackupRepository";

  private static long docsSeed; // see indexDocs()

  @BeforeClass
  public static void seedDocGenerator() {
    docsSeed = random().nextLong();
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
  }

  @Before
  public void clearCollsToDelete() {
    collectionsToDelete = new ArrayList<>();
  }

  @After
  public void deleteTestCollections() throws Exception {
    for (String collName : collectionsToDelete) {
      deleteCollection(collName).process(cluster.getSolrClient());
    }
  }

  private String deleteAfterTest(String collName) {
    collectionsToDelete.add(collName);
    return collName;
  }

  // Populated by 'bootstrapBackupRepositoryData'
  private static int singleShardNumDocs = -1;
  private static int replicasPerShard = -1;
  private static int multiShardNumDocs = -1;
  private static URI singleShard1Uri = null;
  private static URI[] multiShardUris = null;

  private List<String> collectionsToDelete;

  public static void bootstrapBackupRepositoryData(String baseRepositoryLocation) throws Exception {
    final int numShards = /*random().nextInt(3) + 2*/ 4;
    multiShardUris = new URI[numShards];
    replicasPerShard = /*random().nextInt(3) + 1;*/ 3;
    CloudSolrClient solrClient = cluster.getSolrClient();

    // Create collections and index docs
    final String singleShardCollName = createAndAwaitEmptyCollection(1, replicasPerShard);
    singleShardNumDocs = indexDocs(singleShardCollName, true);
    assertCollectionHasNumDocs(singleShardCollName, singleShardNumDocs);
    final String multiShardCollName = createAndAwaitEmptyCollection(numShards, replicasPerShard);
    multiShardNumDocs = indexDocs(multiShardCollName, true);
    assertCollectionHasNumDocs(multiShardCollName, multiShardNumDocs);

    // Upload shard data to BackupRepository - single shard collection
    singleShard1Uri =
        createBackupRepoDirectoryForShardData(
            baseRepositoryLocation, singleShardCollName, "shard1");
    copyShardDataToBackupRepository(singleShardCollName, "shard1", singleShard1Uri);
    // Upload shard data to BackupRepository - multi-shard collection
    for (int i = 0; i < multiShardUris.length; i++) {
      final String shardName = "shard" + (i + 1);
      multiShardUris[i] =
          createBackupRepoDirectoryForShardData(
              baseRepositoryLocation, multiShardCollName, shardName);
      copyShardDataToBackupRepository(multiShardCollName, shardName, multiShardUris[i]);
    }

    // Nuke collections now that we've populated the BackupRepository
    deleteCollection(singleShardCollName).process(solrClient);
    deleteCollection(multiShardCollName).process(solrClient);
  }

  @Test
  public void testInstallFailsIfCollectionIsNotInReadOnlyMode() throws Exception {
    final String collectionName = createAndAwaitEmptyCollection(1, replicasPerShard);
    deleteAfterTest(collectionName);

    final String singleShardLocation = singleShard1Uri.toString();
    final BaseHttpSolrClient.RemoteSolrException rse =
        expectThrows(
            BaseHttpSolrClient.RemoteSolrException.class,
            () -> {
              CollectionAdminRequest.installDataToShard(
                      collectionName, "shard1", singleShardLocation, BACKUP_REPO_NAME)
                  .process(cluster.getSolrClient());
            });
    assertEquals(400, rse.code());
    assertTrue(rse.getMessage().contains("Collection must be in readOnly mode"));

    // Shard-install has failed so collection should still be empty.
    assertCollectionHasNumDocs(collectionName, 0);
  }

  @Test
  public void testInstallToSingleShardCollection() throws Exception {
    final String collectionName = createAndAwaitEmptyCollection(1, replicasPerShard);
    deleteAfterTest(collectionName);
    enableReadOnly(collectionName);

    final String singleShardLocation = singleShard1Uri.toString();
    CollectionAdminRequest.installDataToShard(
            collectionName, "shard1", singleShardLocation, BACKUP_REPO_NAME)
        .process(cluster.getSolrClient());

    // Shard-install has failed so collection should still be empty.
    assertCollectionHasNumDocs(collectionName, singleShardNumDocs);
  }

  @Test
  public void testSerialInstallToMultiShardCollection() throws Exception {
    final String collectionName =
        createAndAwaitEmptyCollection(multiShardUris.length, replicasPerShard);
    deleteAfterTest(collectionName);
    enableReadOnly(collectionName);

    for (int i = 1; i <= multiShardUris.length; i++) {
      CollectionAdminRequest.installDataToShard(
              collectionName, "shard" + i, multiShardUris[i - 1].toString(), BACKUP_REPO_NAME)
          .process(cluster.getSolrClient());
    }

    assertCollectionHasNumDocs(collectionName, multiShardNumDocs);
  }

  @Test
  public void testParallelInstallToMultiShardCollection() throws Exception {
    final String collectionName =
        createAndAwaitEmptyCollection(multiShardUris.length, replicasPerShard);
    deleteAfterTest(collectionName);
    enableReadOnly(collectionName);

    runParallelShardInstalls(collectionName, multiShardUris);

    assertCollectionHasNumDocs(collectionName, multiShardNumDocs);
  }

  /**
   * Builds a string representation of a valid solr.xml configuration, with the provided
   * backup-repository configuration inserted
   *
   * @param backupRepositoryText a string representing the 'backup' XML tag to put in the
   *     constructed solr.xml
   */
  public static String defaultSolrXmlTextWithBackupRepository(String backupRepositoryText) {
    return "<solr>\n"
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
        + "    <str name=\"hostContext\">${hostContext:solr}</str>\n"
        + "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n"
        + "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n"
        + "    <int name=\"leaderVoteWait\">10000</int>\n"
        + "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n"
        + "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n"
        + "  </solrcloud>\n"
        + "  \n"
        + backupRepositoryText
        + "  \n"
        + "</solr>\n";
  }

  private static void assertCollectionHasNumDocs(String collection, int expectedNumDocs)
      throws Exception {
    final SolrClient solrClient = cluster.getSolrClient();
    assertEquals(
        expectedNumDocs,
        solrClient.query(collection, new SolrQuery("*:*")).getResults().getNumFound());
  }

  private static void copyShardDataToBackupRepository(
      String collectionName, String shardName, URI destinationUri) throws Exception {
    final CoreContainer cc = cluster.getJettySolrRunner(0).getCoreContainer();
    final Collection<String> coreNames = cc.getAllCoreNames();
    final String coreName =
        coreNames.stream()
            .filter(name -> name.contains(collectionName) && name.contains(shardName))
            .findFirst()
            .get();
    final CoreDescriptor cd = cc.getCoreDescriptor(coreName);
    final Path coreInstanceDir = cd.getInstanceDir();
    assert coreInstanceDir.toFile().exists();
    assert coreInstanceDir.toFile().isDirectory();

    final Path coreIndexDir = coreInstanceDir.resolve("data").resolve("index");
    assert coreIndexDir.toFile().exists();
    assert coreIndexDir.toFile().isDirectory();

    try (final BackupRepository backupRepository = cc.newBackupRepository(BACKUP_REPO_NAME);
        final SolrCore core = cc.getCore(coreName)) {
      final Directory dir =
          core.getDirectoryFactory()
              .get(
                  coreIndexDir.toString(),
                  DirectoryFactory.DirContext.DEFAULT,
                  core.getSolrConfig().indexConfig.lockType);
      try {
        for (final String dirContent : dir.listAll()) {
          if (dirContent.contains("write.lock")) continue;
          backupRepository.copyFileFrom(dir, dirContent, destinationUri);
        }
      } finally {
        core.getDirectoryFactory().release(dir);
      }
    }
  }

  private static URI createBackupRepoDirectoryForShardData(
      String baseLocation, String collectionName, String shardName) throws Exception {
    final CoreContainer cc = cluster.getJettySolrRunner(0).getCoreContainer();
    try (final BackupRepository backupRepository = cc.newBackupRepository(BACKUP_REPO_NAME)) {
      final URI baseLocationUri = backupRepository.createURI(baseLocation);
      final URI collectionLocation = backupRepository.resolve(baseLocationUri, collectionName);
      backupRepository.createDirectory(collectionLocation);
      final URI shardLocation = backupRepository.resolve(collectionLocation, shardName);
      backupRepository.createDirectory(shardLocation);
      return shardLocation;
    }
  }

  private static int indexDocs(String collectionName, boolean useUUID) throws Exception {
    Random random =
        new Random(
            docsSeed); // use a constant seed for the whole test run so that we can easily re-index.
    int numDocs = random.nextInt(100) + 5;
    indexDocs(collectionName, numDocs, useUUID);
    return numDocs;
  }

  private static void indexDocs(String collectionName, int numDocs, boolean useUUID)
      throws Exception {
    List<SolrInputDocument> docs = new ArrayList<>(numDocs);
    for (int i = 0; i < numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", (useUUID ? java.util.UUID.randomUUID().toString() : i));
      doc.addField("val_s", "some value");
      docs.add(doc);
    }

    CloudSolrClient client = cluster.getSolrClient();
    client.add(collectionName, docs); // batch
    client.commit(collectionName);

    log.info("Indexed {} docs to collection: {}", numDocs, collectionName);
  }

  private static String createAndAwaitEmptyCollection(int numShards, int replicasPerShard)
      throws Exception {
    final SolrClient solrClient = cluster.getSolrClient();

    final String collectionName = UUID.randomUUID().toString().replace("-", "_");
    CollectionAdminRequest.createCollection(collectionName, "conf1", numShards, replicasPerShard)
        .process(solrClient);
    cluster.waitForActiveCollection(collectionName, numShards, numShards * replicasPerShard);

    assertCollectionHasNumDocs(collectionName, 0);
    return collectionName;
  }

  private static void enableReadOnly(String collectionName) throws Exception {
    CollectionAdminRequest.modifyCollection(collectionName, Map.of("readOnly", true))
        .process(cluster.getSolrClient());
  }

  private void runParallelShardInstalls(String collectionName, URI[] dataLocations)
      throws Exception {
    final SolrClient solrClient = cluster.getSolrClient();
    final List<Callable<Exception>> tasks = new ArrayList<>();
    for (int i = 0; i < multiShardUris.length; i++) {
      final String shardName = "shard" + (i + 1);
      final String dataLocation = multiShardUris[i].toString();
      tasks.add(
          () -> {
            try {
              CollectionAdminRequest.installDataToShard(
                      collectionName, shardName, dataLocation, BACKUP_REPO_NAME)
                  .process(solrClient);
              return null;
            } catch (Exception e) {
              return e;
            }
          });
    }

    final ExecutorService executor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            multiShardUris.length, new SolrNamedThreadFactory("shardinstall"));
    // TODO Reduce timeout once PR #1545 is merged to fix S3Mock slowness
    final List<Future<Exception>> futures = executor.invokeAll(tasks, 30, TimeUnit.SECONDS);
    try {
      futures.stream()
          .forEach(
              future -> {
                assertTrue("Shard installation exceeded the test timeout", future.isDone());
                try {
                  assertFalse(
                      "Shard installation was cancelled after timing out.", future.isCancelled());
                  final Exception e = future.get();
                  assertNull("Shard installation failed with exception " + e, e);
                } catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });

      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }
}
