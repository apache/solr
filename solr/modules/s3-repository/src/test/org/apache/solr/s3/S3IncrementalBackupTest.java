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

package org.apache.solr.s3;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.api.collections.AbstractIncrementalBackupTest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.Nightly
@LuceneTestCase.SuppressCodecs({"SimpleText"})
@ThreadLeakLingering(linger = 10)
@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      S3MockTestcontainersThreadFilter.class
    })
@LogLevel(
    value =
        "org.apache.solr.cloud=DEBUG;org.apache.solr.cloud.api.collections=DEBUG;org.apache.solr.cloud.overseer=DEBUG")
public class S3IncrementalBackupTest extends AbstractIncrementalBackupTest {

  private static final String BUCKET_NAME = S3IncrementalBackupTest.class.getSimpleName();

  @ClassRule
  public static final S3MockContainerRule s3MockContainer = new S3MockContainerRule(BUCKET_NAME);

  public static final String SOLR_XML =
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
          + "  </solrcloud>\n"
          + "  \n"
          + "  <backup>\n"
          + "    <repository name=\"errorBackupRepository\" class=\""
          + ErrorThrowingTrackingBackupRepository.class.getName()
          + "\"> \n"
          + "      <str name=\"delegateRepoName\">s3</str>\n"
          + "      <str name=\"hostPort\">${hostPort:8983}</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"trackingBackupRepository\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">s3</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"s3\" class=\"org.apache.solr.s3.S3BackupRepository\"> \n"
          + "      <str name=\"s3.bucket.name\">BUCKET</str>\n"
          + "      <str name=\"s3.region\">REGION</str>\n"
          + "      <str name=\"s3.endpoint\">ENDPOINT</str>\n"
          + "    </repository>\n"
          + "  </backup>\n"
          + "  \n"
          + "</solr>\n";

  @BeforeClass
  public static void ensureCompatibleLocale() {
    // TODO: Find incompatible locales
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");
    // Enable parallel backup/restore for cloud storage tests
    System.setProperty("solr.backup.maxparalleluploads", "2");
    System.setProperty("solr.backup.maxparalleldownloads", "2");
    String retryMode;
    switch (random().nextInt(3)) {
      case 0:
        retryMode = "legacy";
        break;
      case 1:
        retryMode = "standard";
        break;
      default:
        retryMode = "adaptive";
        break;
    }
    System.setProperty("aws.retryMode", retryMode);

    AbstractS3ClientTest.setS3ConfFile();

    configureCluster(NUM_NODES) // nodes
        .addConfig("conf1", getFile("conf/solrconfig.xml").getParent())
        .withSolrXml(
            SOLR_XML
                // Only a single node will have a bad bucket name, all else should succeed.
                // The bad node will be added later
                .replace("BAD_BUCKET_ALL_BUT_ONE", "non-existent")
                .replace("BAD_BUCKET_ONE", BUCKET_NAME)
                .replace("BAD_BUCKET", BUCKET_NAME)
                .replace("BUCKET", BUCKET_NAME)
                .replace("REGION", Region.US_EAST_1.id())
                .replace("ENDPOINT", s3MockContainer.getHttpEndpoint()))
        .configure();
  }

  @Override
  public String getCollectionNamePrefix() {
    return "backuprestore";
  }

  @Override
  public String getBackupLocation() {
    return "/";
  }

  /**
   * Copying a backup to another bucket or prefix with tools such as {@code aws s3 sync} only copies
   * the real files, not zero-byte directory marker objects. Such a copy, configset included, must
   * still restore.
   */
  @Test
  public void testRestoreBackupCopiedWithoutDirectoryMarkers() throws Exception {
    setTestSuffix("testcopiedwithoutmarkers");
    final String backupCollectionName = getCollectionName();
    final String restoreCollectionName = backupCollectionName + "_restore";
    final String restoreConfigName = "conf1_restored";
    final String backupName = BACKUPNAME_PREFIX + testSuffix;
    final String copyLocation = "copied-backups";

    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(backupCollectionName, "conf1", NUM_SHARDS, 1)
        .process(solrClient);
    int numDocs = indexDocs(backupCollectionName, true);
    CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
        .setLocation(getBackupLocation())
        .setRepositoryName(BACKUP_REPO_NAME)
        .processAndWait(solrClient, 100);

    try (S3Client s3 = s3MockContainer.createS3ClientV2()) {
      List<String> keys =
          s3
              .listObjectsV2Paginator(b -> b.bucket(BUCKET_NAME).prefix(backupName + "/"))
              .contents()
              .stream()
              .map(S3Object::key)
              .toList();
      assertTrue(
          "Backup should contain the configset",
          keys.stream().anyMatch(k -> k.contains("/configs/conf1/")));
      assertFalse(
          "Backup should not contain directory marker objects: " + keys,
          keys.stream().anyMatch(k -> k.endsWith("/")));

      for (String key : keys) {
        s3.copyObject(
            b ->
                b.sourceBucket(BUCKET_NAME)
                    .sourceKey(key)
                    .destinationBucket(BUCKET_NAME)
                    .destinationKey(copyLocation + "/" + key));
      }
    }

    // Restoring under a new config name makes the restore upload the configset from the backup
    CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
        .setLocation(copyLocation)
        .setConfigName(restoreConfigName)
        .setRepositoryName(BACKUP_REPO_NAME)
        .processAndWait(solrClient, 500);
    AbstractFullDistribZkTestBase.waitForRecoveriesToFinish(
        restoreCollectionName, ZkStateReader.from(solrClient), false, false, 3);

    assertTrue(
        new ConfigSetAdminRequest.List()
            .process(solrClient)
            .getConfigSets()
            .contains(restoreConfigName));
    assertEquals(
        numDocs,
        solrClient.query(restoreCollectionName, new SolrQuery("*:*")).getResults().getNumFound());
  }

  /**
   * S3 can't hold an empty directory, so a backup location doesn't have to be created first: it
   * comes into existence with the first file of the backup. Restoring from a location that holds
   * nothing still fails.
   */
  @Test
  public void testBackupToLocationThatDoesNotExistYet() throws Exception {
    setTestSuffix("testnewlocation");
    final String backupCollectionName = getCollectionName();
    final String restoreCollectionName = backupCollectionName + "_restore";
    final String backupName = BACKUPNAME_PREFIX + testSuffix;
    final String newLocation = "new-location/nested";

    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(backupCollectionName, "conf1", NUM_SHARDS, 1)
        .process(solrClient);
    int numDocs = indexDocs(backupCollectionName, true);
    assertEquals(
        RequestStatusState.COMPLETED,
        CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
            .setLocation(newLocation)
            .setRepositoryName(BACKUP_REPO_NAME)
            .processAndWait(solrClient, 100));

    assertEquals(
        RequestStatusState.COMPLETED,
        CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
            .setLocation(newLocation)
            .setRepositoryName(BACKUP_REPO_NAME)
            .processAndWait(solrClient, 500));
    AbstractFullDistribZkTestBase.waitForRecoveriesToFinish(
        restoreCollectionName, ZkStateReader.from(solrClient), false, false, 3);
    assertEquals(
        numDocs,
        solrClient.query(restoreCollectionName, new SolrQuery("*:*")).getResults().getNumFound());

    Exception e =
        expectThrows(
            Exception.class,
            () ->
                CollectionAdminRequest.restoreCollection(restoreCollectionName + "_2", backupName)
                    .setLocation("missing-location")
                    .setRepositoryName(BACKUP_REPO_NAME)
                    .processAndWait(solrClient, 100));
    assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
  }
}
