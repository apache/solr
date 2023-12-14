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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
public class LocalFSForbiddenPathBackupTest extends SolrCloudTestCase {
  private static final String SOLR_XML =
      "<solr>\n"
          + "\n"
          + "  <str name=\"allowPaths\">ALLOWPATHS_TEMPLATE_VAL</str>\n"
          + "  <str name=\"sharedLib\">SHAREDLIB_TEMPLATE_VAL</str>\n"
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
          + "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n"
          + "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n"
          + "    <int name=\"leaderVoteWait\">10000</int>\n"
          + "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n"
          + "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n"
          + "  </solrcloud>\n"
          + "  \n"
          + "  <backup>\n"
          + "    <repository name=\"trackingBackupRepository\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">localfs</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"localfs\" class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\"> \n"
          + "    </repository>\n"
          + "  </backup>\n"
          + "  \n"
          + "</solr>\n";

  private static Path sharedLibPath;
  private static String backupCollectionName = "test";

  @BeforeClass
  public static void setupClass() throws Exception {
    boolean whitespacesInPath = random().nextBoolean();
    if (whitespacesInPath) {
      sharedLibPath = createTempDir("shared dir").resolve("lib").toAbsolutePath();
    } else {
      sharedLibPath = createTempDir("shared").resolve("lib").toAbsolutePath();
    }

    configureCluster(1) // nodes
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(
            SOLR_XML
                .replaceAll("SHAREDLIB_TEMPLATE_VAL", sharedLibPath.toString())
                .replaceAll("ALLOWPATHS_TEMPLATE_VAL", sharedLibPath.getParent().toString()))
        .configure();

    CollectionAdminRequest.createCollection(backupCollectionName, "conf1", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testHomeLibDir() throws Exception {
    Path homeDir = Path.of(cluster.getJettySolrRunner(0).getSolrHome()).toAbsolutePath();

    testProtectedDirectory(homeDir);

    // We should be able to create the backup under a different directory in SOLR_HOME
    Path subHomePath = homeDir.resolve("another");
    Files.createDirectory(subHomePath);

    CollectionAdminResponse resp =
        CollectionAdminRequest.backupCollection(backupCollectionName, "test")
            .setLocation(subHomePath.toString())
            .setRepositoryName("trackingBackupRepository")
            .process(cluster.getSolrClient());

    assertTrue("Backup was not successful: " + resp.getWarning(), resp.isSuccess());
  }

  @Test
  public void testInsideSharedLibDir() throws Exception {
    Path libPath = sharedLibPath.resolve("inner").toAbsolutePath();
    Files.createDirectory(libPath);

    testProtectedDirectory(libPath);
  }

  @Test
  public void testSharedLibDir() throws Exception {
    Path sharedLibDir = sharedLibPath.getParent().toAbsolutePath();

    testProtectedDirectory(sharedLibDir);

    // We should be able to create the backup under a different directory next to the Shared Lib dir
    Path subHomePath = sharedLibDir.resolve("another");
    Files.createDirectory(subHomePath);

    CollectionAdminResponse resp =
        CollectionAdminRequest.backupCollection(backupCollectionName, "test")
            .setLocation(subHomePath.toString())
            .setRepositoryName("trackingBackupRepository")
            .process(cluster.getSolrClient());

    assertTrue("Backup was not successful: " + resp.getWarning(), resp.isSuccess());
  }

  @Test
  public void testCoreHomeLibDir() throws Exception {
    Path coreDir =
        cluster.getJettySolrRunners().get(0).getCoreContainer().getCores().get(0).getInstancePath();

    testProtectedDirectory(coreDir);
  }

  private void testProtectedDirectory(Path directory) throws Exception {
    assertExceptionThrownWithMessageContaining(
        BaseHttpSolrClient.RemoteSolrException.class,
        List.of("cannot be used to write files"),
        () ->
            CollectionAdminRequest.backupCollection(backupCollectionName, "lib")
                .setLocation(directory.toString())
                .setRepositoryName("trackingBackupRepository")
                .process(cluster.getSolrClient()));

    Path libPath = directory.resolve("lib");
    Files.createDirectory(libPath);

    assertExceptionThrownWithMessageContaining(
        BaseHttpSolrClient.RemoteSolrException.class,
        List.of("cannot be used to write files"),
        () ->
            CollectionAdminRequest.backupCollection(backupCollectionName, "test")
                .setLocation(libPath.toString())
                .setRepositoryName("trackingBackupRepository")
                .process(cluster.getSolrClient()));
  }
}
