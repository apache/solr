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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.BeforeClass;
import org.junit.Test;

// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
public class LocalFSCloudIncrementalBackupTest extends AbstractIncrementalBackupTest {
  private static final String SOLR_XML =
      "<solr>\n"
          + "\n"
          + "  <str name=\"allowPaths\">ALLOWPATHS_TEMPLATE_VAL</str>\n"
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
          + "      <str name=\"delegateRepoName\">localfs</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"localfs\" class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\"> \n"
          + "    </repository>\n"
          + "  </backup>\n"
          + "  \n"
          + "</solr>\n";

  private static String backupLocation;

  @BeforeClass
  public static void setupClass() throws Exception {
    boolean whitespacesInPath = random().nextBoolean();
    if (whitespacesInPath) {
      backupLocation = createTempDir("my backup").toAbsolutePath().toString();
    } else {
      backupLocation = createTempDir("mybackup").toAbsolutePath().toString();
    }

    configureCluster(NUM_SHARDS) // nodes
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(SOLR_XML.replace("ALLOWPATHS_TEMPLATE_VAL", backupLocation))
        .configure();
  }

  @Override
  public String getCollectionNamePrefix() {
    return "backuprestore";
  }

  @Override
  public String getBackupLocation() {
    return backupLocation;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCustomProperties() throws Exception {
    setTestSuffix("testCustomProperties");
    final String backupCollectionName = getCollectionName();
    final String restoreCollectionName = backupCollectionName + "_restore";

    CloudSolrClient solrClient = cluster.getSolrClient();

    CollectionAdminRequest.createCollection(backupCollectionName, "conf1", NUM_SHARDS, 1)
        .process(solrClient);
    int numDocs = indexDocs(backupCollectionName, true);
    String backupName = BACKUPNAME_PREFIX + testSuffix;
    try (BackupRepository repository =
        cluster.getJettySolrRunner(0).getCoreContainer().newBackupRepository(BACKUP_REPO_NAME)) {
      String backupLocation = repository.getBackupLocation(getBackupLocation());
      Properties extraProps = new Properties();
      extraProps.putAll(Map.of("foo", "bar", "number", "12345"));
      CollectionAdminRequest.backupCollection(backupCollectionName, backupName)
          .setLocation(backupLocation)
          .setExtraProperties(extraProps)
          .setRepositoryName(BACKUP_REPO_NAME)
          .processAndWait(cluster.getSolrClient(), 100);
      CollectionAdminResponse response =
          CollectionAdminRequest.listBackup(backupName)
              .setBackupLocation(backupLocation)
              .setBackupRepository(BACKUP_REPO_NAME)
              .process(cluster.getSolrClient());
      assertNotNull(response.getResponse().get("backups"));
      assertTrue(response.getResponse().get("backups") instanceof List);
      List<Map<String, Object>> backups =
          (List<Map<String, Object>>) response.getResponse().get("backups");
      assertEquals(1, backups.size());
      Map<String, Object> backup0 = backups.get(0);
      assertNotNull(backup0.get("extraProperties"));
      assertTrue(backup0.get("extraProperties") instanceof Map);
      Map<String, Object> extraProperties = (Map<String, Object>) backup0.get("extraProperties");
      assertEquals("bar", extraProperties.get("foo"));
      assertEquals("12345", extraProperties.get("number"));

      CollectionAdminRequest.restoreCollection(restoreCollectionName, backupName)
          .setLocation(backupLocation)
          .setRepositoryName(BACKUP_REPO_NAME)
          .processAndWait(solrClient, 500);

      AbstractDistribZkTestBase.waitForRecoveriesToFinish(
          restoreCollectionName, ZkStateReader.from(solrClient), false, false, 3);
      assertEquals(
          numDocs,
          cluster
              .getSolrClient()
              .query(restoreCollectionName, new SolrQuery("*:*"))
              .getResults()
              .getNumFound());
    }
  }
}
