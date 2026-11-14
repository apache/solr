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
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.cloud.api.collections.AbstractConfigSetUploadFromRepositoryTest;
import org.apache.solr.cloud.api.collections.AbstractInstallShardTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import software.amazon.awssdk.regions.Region;

/** Tests uploading configsets from a backup repository with {@link S3BackupRepository}. */
@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      S3MockTestcontainersThreadFilter.class
    })
public class S3ConfigSetUploadFromRepositoryTest extends AbstractConfigSetUploadFromRepositoryTest {

  private static final String BUCKET_NAME =
      S3ConfigSetUploadFromRepositoryTest.class.getSimpleName();

  private static final String REPOSITORY_NAME = "s3";

  private static final String BACKUP_REPOSITORY_XML =
      "  <backup>\n"
          + "    <repository name=\"s3\" class=\"org.apache.solr.s3.S3BackupRepository\"> \n"
          + "      <str name=\"s3.bucket.name\">BUCKET</str>\n"
          + "      <str name=\"s3.region\">REGION</str>\n"
          + "      <str name=\"s3.endpoint\">ENDPOINT</str>\n"
          + "    </repository>\n"
          + "  </backup>\n";

  @ClassRule
  public static final S3MockContainerRule s3MockContainer = new S3MockContainerRule(BUCKET_NAME);

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");

    AbstractS3ClientTest.setS3ConfFile();

    configureCluster(1)
        .withSolrXml(
            AbstractInstallShardTest.defaultSolrXmlTextWithBackupRepository(BACKUP_REPOSITORY_XML)
                .replace("BUCKET", BUCKET_NAME)
                .replace("REGION", Region.US_EAST_1.id())
                .replace("ENDPOINT", s3MockContainer.getHttpEndpoint()))
        .configure();
  }

  @Override
  protected String repositoryName() {
    return REPOSITORY_NAME;
  }

  @Override
  protected String baseLocation() {
    return "/";
  }
}
