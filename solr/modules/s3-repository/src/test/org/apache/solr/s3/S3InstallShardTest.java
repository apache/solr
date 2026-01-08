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
import java.util.concurrent.TimeUnit;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.api.collections.AbstractInstallShardTest;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.admin.api.InstallShardData;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;

/**
 * Tests validating that the 'Install Shard API' works when used with {@link S3BackupRepository}
 *
 * @see org.apache.solr.cloud.api.collections.AbstractInstallShardTest
 * @see InstallShardData
 */
// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
@ThreadLeakLingering(linger = 10)
public class S3InstallShardTest extends AbstractInstallShardTest {

  private static final String BUCKET_NAME = S3InstallShardTest.class.getSimpleName();

  private static final String BACKUP_REPOSITORY_XML =
      "  <backup>\n"
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
          + "      <str name=\"s3.bucket.name\">BAD_BUCKET</str>\n"
          + "      <str name=\"s3.region\">REGION</str>\n"
          + "      <str name=\"s3.endpoint\">ENDPOINT</str>\n"
          + "    </repository>\n"
          + "  </backup>\n";
  private static final String SOLR_XML =
      AbstractInstallShardTest.defaultSolrXmlTextWithBackupRepository(BACKUP_REPOSITORY_XML);

  @ClassRule
  @SuppressWarnings("removal")
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder().withInitialBuckets(BUCKET_NAME).withSecureConnection(false).build();

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");

    AbstractS3ClientTest.setS3ConfFile();

    configureCluster(1) // nodes
        .addConfig("conf1", getFile("conf/solrconfig.xml").getParent())
        .withSolrXml(
            SOLR_XML
                // The first solr node will not have a bad bucket
                .replace("BAD_BUCKET", BUCKET_NAME)
                .replace("BUCKET", BUCKET_NAME)
                .replace("REGION", Region.US_EAST_1.id())
                .replace("ENDPOINT", "http://localhost:" + S3_MOCK_RULE.getHttpPort()))
        .configure();

    bootstrapBackupRepositoryData("/");
  }

  @Test
  public void testInstallSucceedsOnASingleError() throws Exception {
    JettySolrRunner jettySolrRunner =
        cluster.startJettySolrRunner(
            SOLR_XML
                // The first solr node will not have a bad bucket
                .replace("BAD_BUCKET", "non-existent")
                .replace("BUCKET", BUCKET_NAME)
                .replace("REGION", Region.US_EAST_1.id())
                .replace("ENDPOINT", "http://localhost:" + S3_MOCK_RULE.getHttpPort()));

    try {
      final String collectionName = createAndAwaitEmptyCollection(1, 2);
      deleteAfterTest(collectionName);
      enableReadOnly(collectionName);

      final String singleShardLocation = singleShard1Uri.toString();
      { // Test synchronous request error reporting
        CollectionAdminRequest.installDataToShard(
                collectionName, "shard1", singleShardLocation, BACKUP_REPO_NAME + "BadNode")
            .process(cluster.getSolrClient());
        waitForState(
            "The failed core-install should recover and become healthy",
            collectionName,
            30,
            TimeUnit.SECONDS,
            SolrCloudTestCase.activeClusterShape(1, 2));
        assertCollectionHasNumDocs(collectionName, singleShardNumDocs);
      }

      { // Test asynchronous request error reporting
        final var requestStatusState =
            CollectionAdminRequest.installDataToShard(
                    collectionName, "shard1", singleShardLocation, BACKUP_REPO_NAME + "BadNode")
                .processAndWait(cluster.getSolrClient(), 15);

        assertEquals(RequestStatusState.COMPLETED, requestStatusState);
        waitForState(
            "The failed core-install should recover and become healthy",
            collectionName,
            30,
            TimeUnit.SECONDS,
            SolrCloudTestCase.activeClusterShape(1, 2));
        assertCollectionHasNumDocs(collectionName, singleShardNumDocs);
      }
    } finally {
      jettySolrRunner.stop();
    }
  }
}
