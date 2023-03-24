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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.cloud.api.collections.AbstractInstallShardTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import software.amazon.awssdk.regions.Region;

/**
 * Tests validating that the 'Install Shard API' works when used with {@link S3BackupRepository}
 *
 * @see org.apache.solr.cloud.api.collections.AbstractInstallShardTest
 * @see org.apache.solr.handler.admin.api.InstallShardDataAPI
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
          + "  </backup>\n";
  private static final String SOLR_XML =
      AbstractInstallShardTest.defaultSolrXmlTextWithBackupRepository(BACKUP_REPOSITORY_XML);

  @ClassRule
  public static final S3MockRule S3_MOCK_RULE =
      S3MockRule.builder()
          .silent()
          .withInitialBuckets(BUCKET_NAME)
          .withSecureConnection(false)
          .build();

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");

    configureCluster(1) // nodes
        .addConfig("conf1", getFile("conf/solrconfig.xml").getParentFile().toPath())
        .withSolrXml(
            SOLR_XML
                .replace("BUCKET", BUCKET_NAME)
                .replace("REGION", Region.US_EAST_1.id())
                .replace("ENDPOINT", "http://localhost:" + S3_MOCK_RULE.getHttpPort()))
        .configure();

    bootstrapBackupRepositoryData("/");
  }
}
