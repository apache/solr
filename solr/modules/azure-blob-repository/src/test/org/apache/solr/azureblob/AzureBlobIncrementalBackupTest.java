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
package org.apache.solr.azureblob;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.cloud.api.collections.AbstractIncrementalBackupTest;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

/**
 * Runs the shared {@link AbstractIncrementalBackupTest} SolrCloud suite against {@link
 * AzureBlobBackupRepository}, backed by an Azurite emulator.
 */
// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
@ThreadLeakLingering(linger = 10)
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      AbstractAzureBlobClientTest.OkHttpThreadLeakFilterTest.class,
    })
@LogLevel(
    value =
        "org.apache.solr.cloud=DEBUG;org.apache.solr.cloud.api.collections=DEBUG;org.apache.solr.cloud.overseer=DEBUG")
public class AzureBlobIncrementalBackupTest extends AbstractIncrementalBackupTest {

  private static final String CONTAINER_NAME = "incremental-backup-test";

  private static AzuriteTestContainer azurite;

  private static final String SOLR_XML =
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
          + "    <repository name=\"errorBackupRepository\" class=\""
          + ErrorThrowingTrackingBackupRepository.class.getName()
          + "\"> \n"
          + "      <str name=\"delegateRepoName\">azure</str>\n"
          + "      <str name=\"hostPort\">${hostPort:8983}</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"trackingBackupRepository\" class=\"org.apache.solr.core.TrackingBackupRepository\"> \n"
          + "      <str name=\"delegateRepoName\">azure</str>\n"
          + "    </repository>\n"
          + "    <repository name=\"azure\" class=\"org.apache.solr.azureblob.AzureBlobBackupRepository\"> \n"
          + "      <str name=\"azure.blob.container.name\">CONTAINER</str>\n"
          + "      <str name=\"azure.blob.connection.string\">CONNECTION_STRING</str>\n"
          + "    </repository>\n"
          + "  </backup>\n"
          + "  \n"
          + "</solr>\n";

  @BeforeClass
  public static void ensureCompatibleLocale() {
    // TODO: Find incompatible locales
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    try {
      azurite = AzuriteTestContainer.start();
    } catch (Throwable t) {
      Assume.assumeNoException("Docker/Testcontainers not available; skipping Azure tests", t);
    }
    azurite.createContainerIfMissing(CONTAINER_NAME);

    // Enable parallel backup/restore for cloud storage tests
    System.setProperty("solr.backup.maxparalleluploads", "2");
    System.setProperty("solr.backup.maxparalleldownloads", "2");

    configureCluster(NUM_NODES) // nodes
        .addConfig("conf1", getFile("conf/solrconfig.xml").getParent())
        .withSolrXml(
            SOLR_XML
                .replace("CONTAINER", CONTAINER_NAME)
                .replace("CONNECTION_STRING", azurite.connectionString()))
        .configure();
  }

  @AfterClass
  public static void tearDownClass() {
    if (azurite != null) {
      azurite.stop();
      azurite = null;
    }
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
