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

import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.api.collections.AbstractIncrementalBackupTest;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
@ThreadLeakLingering(linger = 10)
public class S3IncrementalBackupTest extends AbstractIncrementalBackupTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String BUCKET_NAME = S3IncrementalBackupTest.class.getSimpleName();

  @ClassRule
  @SuppressWarnings("removal")
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().withInitialBuckets(BUCKET_NAME).withSecureConnection(false).build();

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
          + "    <repository name=\"trackingBackupRepository\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">s3</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"s3\" class=\"org.apache.solr.s3.S3BackupRepository\"> \n"
          + "      <str name=\"s3.bucket.name\">BUCKET</str>\n"
          + "      <str name=\"s3.region\">REGION</str>\n"
          + "      <str name=\"s3.endpoint\">ENDPOINT</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"trackingBackupRepositoryBadNode\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">s3BadNode</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"s3BadNode\" class=\"org.apache.solr.s3.S3BackupRepository\"> \n"
          + "      <str name=\"s3.bucket.name\">BAD_BUCKET_ONE</str>\n"
          + "      <str name=\"s3.region\">REGION</str>\n"
          + "      <str name=\"s3.endpoint\">ENDPOINT</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"trackingBackupRepositoryBadNodes\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">s3BadNodes</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"s3BadNodes\" class=\"org.apache.solr.s3.S3BackupRepository\"> \n"
          + "      <str name=\"s3.bucket.name\">BAD_BUCKET_ALL_BUT_ONE</str>\n"
          + "      <str name=\"s3.region\">REGION</str>\n"
          + "      <str name=\"s3.endpoint\">ENDPOINT</str>\n"
          + "    </repository>\n"
          + "  </backup>\n"
          + "  \n"
          + "</solr>\n";

  private static String backupLocation;

  @BeforeClass
  public static void ensureCompatibleLocale() {
    // TODO: Find incompatible locales
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");
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
                .replace("ENDPOINT", "http://localhost:" + S3_MOCK_RULE.getHttpPort()))
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

  @Test
  public void testRestoreToOriginalSucceedsOnASingleError() throws Exception {
    JettySolrRunner badNodeJetty =
        cluster.startJettySolrRunner(
            SOLR_XML
                // The first solr node will not have a bad bucket
                .replace("BAD_BUCKET_ALL_BUT_ONE", BUCKET_NAME)
                .replace("BAD_BUCKET_ONE", "non-existent")
                .replace("BUCKET", BUCKET_NAME)
                .replace("REGION", Region.US_EAST_1.id())
                .replace("ENDPOINT", "http://localhost:" + S3_MOCK_RULE.getHttpPort()));

    try {
      setTestSuffix("testRestoreToOriginalSucceedsOnASingleError");
      final String backupCollectionName = getCollectionName();
      final String backupName = BACKUPNAME_PREFIX + testSuffix;

      // Bootstrap the backup collection with seed docs
      CollectionAdminRequest.createCollection(
              backupCollectionName, "conf1", NUM_SHARDS, NUM_NODES + 1)
          .process(cluster.getSolrClient());
      final int firstBatchNumDocs = indexDocs(backupCollectionName, true);

      // Backup and immediately add more docs to the collection
      try (BackupRepository repository =
          cluster.getJettySolrRunner(0).getCoreContainer().newBackupRepository(BACKUP_REPO_NAME)) {
        final String backupLocation = repository.getBackupLocation(getBackupLocation());
        final RequestStatusState result =
            CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
                .setBackupConfigset(false)
                .setLocation(backupLocation)
                .setRepositoryName(BACKUP_REPO_NAME)
                .processAndWait(cluster.getSolrClient(), 20);
        assertEquals(RequestStatusState.COMPLETED, result);
      }
      int secondBatchNumDocs = indexDocs(backupCollectionName, true);
      int maxDocs = secondBatchNumDocs + firstBatchNumDocs;
      assertEquals(maxDocs, getNumDocsInCollection(backupCollectionName));

      /*
      Restore original docs and validate that doc count is correct
      */
      // Test a single bad node
      try (BackupRepository repository =
              cluster
                  .getJettySolrRunner(0)
                  .getCoreContainer()
                  .newBackupRepository(BACKUP_REPO_NAME);
          SolrClient goodNodeClient = cluster.getJettySolrRunner(0).newClient()) {
        final String backupLocation = repository.getBackupLocation(getBackupLocation());
        final RequestStatusState result =
            CollectionAdminRequest.restoreCollection(backupCollectionName, backupName)
                .setLocation(backupLocation)
                .setRepositoryName(BACKUP_REPO_NAME + "BadNode")
                .processAndWait(goodNodeClient, 30);
        assertEquals(RequestStatusState.COMPLETED, result);
        waitForState(
            "The failed core-install should recover and become healthy",
            backupCollectionName,
            30,
            TimeUnit.SECONDS,
            SolrCloudTestCase.activeClusterShape(NUM_SHARDS, NUM_SHARDS * (NUM_NODES + 1)));
      }
      assertEquals(firstBatchNumDocs, getNumDocsInCollection(backupCollectionName));
      secondBatchNumDocs = indexDocs(backupCollectionName, true);
      maxDocs = secondBatchNumDocs + firstBatchNumDocs;
      assertEquals(maxDocs, getNumDocsInCollection(backupCollectionName));

      // Test a single good node
      try (BackupRepository repository =
              cluster
                  .getJettySolrRunner(0)
                  .getCoreContainer()
                  .newBackupRepository(BACKUP_REPO_NAME);
          SolrClient goodNodeClient = badNodeJetty.newClient()) {
        final String backupLocation = repository.getBackupLocation(getBackupLocation());
        final RequestStatusState result =
            CollectionAdminRequest.restoreCollection(backupCollectionName, backupName)
                .setLocation(backupLocation)
                .setRepositoryName(BACKUP_REPO_NAME + "BadNodes")
                .processAndWait(goodNodeClient, 30);
        assertEquals(RequestStatusState.COMPLETED, result);
        waitForState(
            "The failed core-install should recover and become healthy",
            backupCollectionName,
            30,
            TimeUnit.SECONDS,
            SolrCloudTestCase.activeClusterShape(NUM_SHARDS, NUM_SHARDS * (NUM_NODES + 1)));
      }
      assertEquals(firstBatchNumDocs, getNumDocsInCollection(backupCollectionName));
    } finally {
      badNodeJetty.stop();
    }
  }
}
