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
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Locale;

import io.findify.s3mock.S3Mock;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.cloud.api.collections.AbstractIncrementalBackupTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@LuceneTestCase.SuppressCodecs({
  "SimpleText"
}) // Backups do checksum validation against a footer value not present in 'SimpleText'
public class S3IncrementalBackupTest extends AbstractIncrementalBackupTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String BUCKET_NAME = S3IncrementalBackupTest.class.getSimpleName().toLowerCase(Locale.ROOT);

  private static S3Mock s3Mock;
  private static int s3MockPort;

  @BeforeClass
  public static void setUpS3Mock() throws Exception {
    s3Mock = S3Mock.create(0);
    s3MockPort = s3Mock.start().localAddress().getPort();

    try (S3Client s3 = S3Client.builder()
        .endpointOverride(URI.create("http://localhost:" + s3MockPort))
        .region(Region.of("us-west-2"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .serviceConfiguration(builder -> builder.pathStyleAccessEnabled(true))
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .build()) {
      s3.createBucket(r -> r.bucket(BUCKET_NAME));
    }

    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");

    configureCluster(NUM_SHARDS) // nodes
        .addConfig("conf1", getFile("conf/solrconfig.xml").getParentFile().toPath())
        .withSolrXml(
            SOLR_XML
                .replace("BUCKET", BUCKET_NAME)
                .replace("REGION", Region.US_EAST_1.id())
                .replace("ENDPOINT", "http://localhost:" + s3MockPort))
        .configure();
  }

  @AfterClass
  public static void tearDownS3Mock() {
    s3Mock.shutdown();
  }

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
          + "    <str name=\"hostContext\">${hostContext:solr}</str>\n"
          + "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n"
          + "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n"
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
  }

  @Override
  public String getCollectionNamePrefix() {
    return "backuprestore";
  }

  @Override
  public String getBackupLocation() {
    return "/";
  }
}
