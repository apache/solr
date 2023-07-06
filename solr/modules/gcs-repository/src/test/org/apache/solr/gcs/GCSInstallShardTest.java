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

package org.apache.solr.gcs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.cloud.api.collections.AbstractInstallShardTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests validating that the 'Install Shard API' works when used with {@link GCSBackupRepository}
 *
 * @see org.apache.solr.cloud.api.collections.AbstractInstallShardTest
 * @see org.apache.solr.handler.admin.api.InstallShardDataAPI
 */
// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
@ThreadLeakLingering(linger = 10)
public class GCSInstallShardTest extends AbstractInstallShardTest {

  private static final String BACKUP_REPOSITORY_XML =
      "  <backup>\n"
          + "    <repository name=\"trackingBackupRepository\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">localfs</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"localfs\" class=\"org.apache.solr.gcs.LocalStorageGCSBackupRepository\"> \n"
          + "      <str name=\"gcsBucket\">someBucketName</str>\n"
          + "      <str name=\"location\">backup1</str>\n"
          + "    </repository>\n"
          + "  </backup>\n";
  private static final String SOLR_XML =
      AbstractInstallShardTest.defaultSolrXmlTextWithBackupRepository(BACKUP_REPOSITORY_XML);

  @BeforeClass
  public static void setupClass() throws Exception {

    configureCluster(1) // nodes
        .addConfig("conf1", getFile("conf/solrconfig.xml").getParentFile().toPath())
        .withSolrXml(SOLR_XML)
        .configure();

    bootstrapBackupRepositoryData("backup1");
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    LocalStorageGCSBackupRepository.clearStashedStorage();
  }
}
