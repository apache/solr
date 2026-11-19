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
import org.apache.solr.cloud.api.collections.AbstractConfigSetUploadFromRepositoryTest;
import org.apache.solr.cloud.api.collections.AbstractInstallShardTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Tests uploading configsets from a backup repository with {@link GCSBackupRepository}. */
@ThreadLeakLingering(linger = 10)
public class GCSConfigSetUploadFromRepositoryTest
    extends AbstractConfigSetUploadFromRepositoryTest {

  private static final String REPOSITORY_NAME = "localfs";

  private static final String BACKUP_REPOSITORY_XML =
      "  <backup>\n"
          + "    <repository name=\"localfs\" class=\"org.apache.solr.gcs.LocalStorageGCSBackupRepository\"> \n"
          + "      <str name=\"gcsBucket\">someBucketName</str>\n"
          + "      <str name=\"location\">backup1</str>\n"
          + "    </repository>\n"
          + "  </backup>\n";

  @BeforeClass
  public static void setupClass() throws Exception {
    configureCluster(1)
        .withSolrXml(
            AbstractInstallShardTest.defaultSolrXmlTextWithBackupRepository(BACKUP_REPOSITORY_XML))
        .configure();
  }

  @AfterClass
  public static void tearDownClass() {
    LocalStorageGCSBackupRepository.clearStashedStorage();
  }

  @Override
  protected String repositoryName() {
    return REPOSITORY_NAME;
  }

  @Override
  protected String baseLocation() {
    return "backup1";
  }
}
