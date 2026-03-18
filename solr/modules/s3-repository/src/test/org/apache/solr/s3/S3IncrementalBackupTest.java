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
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.cloud.api.collections.AbstractIncrementalBackupTest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.TrackingBackupRepository;
import org.apache.solr.core.backup.repository.BackupRepository;
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

    @Override
    @Test
    public void testSkipConfigset() throws Exception {
    setTestSuffix("testskipconfigset");
    final String backupCollectionName = getCollectionName();
    final String restoreCollectionName = backupCollectionName + "-restore";

    CloudSolrClient solrClient = cluster.getSolrClient();

    CollectionAdminRequest.createCollection(backupCollectionName, "conf1", NUM_SHARDS, 1)
      .process(solrClient);
    int numDocs = indexDocs(backupCollectionName, true);
    String backupName = BACKUPNAME_PREFIX + testSuffix;
    try (BackupRepository repository =
      cluster.getJettySolrRunner(0).getCoreContainer().newBackupRepository(BACKUP_REPO_NAME)) {
      String backupLocation = repository.getBackupLocation(getBackupLocation());
      CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
        .setLocation(backupLocation)
        .setBackupConfigset(false)
        .setRepositoryName(BACKUP_REPO_NAME)
        .processAndWait(cluster.getSolrClient(), 100);

      assertFalse(
        "Configset shouldn't be part of the backup but found:\n"
          + TrackingBackupRepository.directoriesCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.directoriesCreated().stream()
          .anyMatch(f -> f.getPath().contains("configs/conf1")));
      assertFalse(
        "Configset shouldn't be part of the backup but found:\n"
          + TrackingBackupRepository.outputsCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.outputsCreated().stream()
          .anyMatch(f -> f.getPath().contains("configs/conf1")));
      assertFalse(
        "solrconfig.xml shouldn't be part of the backup but found:\n"
          + TrackingBackupRepository.outputsCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.outputsCreated().stream()
          .anyMatch(f -> f.getPath().contains("solrconfig.xml")));
      assertFalse(
        "schema.xml shouldn't be part of the backup but found:\n"
          + TrackingBackupRepository.outputsCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.outputsCreated().stream()
          .anyMatch(f -> f.getPath().contains("schema.xml")));

      CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
        .setLocation(backupLocation)
        .setRepositoryName(BACKUP_REPO_NAME)
        .processAndWait(solrClient, 500);

      AbstractFullDistribZkTestBase.waitForRecoveriesToFinish(
        restoreCollectionName, ZkStateReader.from(solrClient), log.isDebugEnabled(), false, 3);
      assertEquals(
        numDocs,
        cluster
          .getSolrClient()
          .query(restoreCollectionName, new SolrQuery("*:*"))
          .getResults()
          .getNumFound());
    }

    TrackingBackupRepository.clear();

    try (BackupRepository repository =
      cluster.getJettySolrRunner(0).getCoreContainer().newBackupRepository(BACKUP_REPO_NAME)) {
      String backupLocation = repository.getBackupLocation(getBackupLocation());
      CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
        .setLocation(backupLocation)
        .setBackupConfigset(true)
        .setRepositoryName(BACKUP_REPO_NAME)
        .processAndWait(cluster.getSolrClient(), 100);

      // S3 backups should never create explicit directory markers.
      assertFalse(
        "Configset directory creation should not happen for S3 backups:\n"
          + TrackingBackupRepository.directoriesCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.directoriesCreated().stream()
          .anyMatch(f -> f.getPath().contains("configs/conf1")));
      assertTrue(
        "Configset should be part of the backup but not found:\n"
          + TrackingBackupRepository.outputsCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.outputsCreated().stream()
          .anyMatch(f -> f.getPath().contains("configs/conf1")));
      assertTrue(
        "solrconfig.xml should be part of the backup but not found:\n"
          + TrackingBackupRepository.outputsCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.outputsCreated().stream()
          .anyMatch(f -> f.getPath().contains("solrconfig.xml")));
      assertTrue(
        "schema.xml should be part of the backup but not found:\n"
          + TrackingBackupRepository.outputsCreated().stream()
            .map(java.net.URI::toString)
            .collect(Collectors.joining("\n")),
        TrackingBackupRepository.outputsCreated().stream()
          .anyMatch(f -> f.getPath().contains("schema.xml")));
    }
    }
}
