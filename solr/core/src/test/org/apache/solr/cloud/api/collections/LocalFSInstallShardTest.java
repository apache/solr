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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.BeforeClass;

// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
public class LocalFSInstallShardTest extends AbstractInstallShardTest {

  private static final String BACKUP_REPOSITORY_XML =
      "  <str name=\"allowPaths\">ALLOWPATHS_TEMPLATE_VAL</str>\n"
          + "  <backup>\n"
          + "    <repository name=\"trackingBackupRepository\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">localfs</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"localfs\" class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\"> \n"
          + "    </repository>\n"
          + "  </backup>\n";

  private static final String SOLR_XML =
      AbstractInstallShardTest.defaultSolrXmlTextWithBackupRepository(BACKUP_REPOSITORY_XML);

  @BeforeClass
  public static void setupClass() throws Exception {
    boolean whitespacesInPath = random().nextBoolean();
    final String tmpDirPrefix = whitespacesInPath ? "my install" : "myinstall";
    final String backupLocation = createTempDir(tmpDirPrefix).toAbsolutePath().toString();

    configureCluster(1) // nodes
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(SOLR_XML.replace("ALLOWPATHS_TEMPLATE_VAL", backupLocation))
        .configure();

    bootstrapBackupRepositoryData(backupLocation);
  }
}
