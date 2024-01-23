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

package org.apache.solr.zero.process;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

/** Tests for {@link CorePusher}. */
// Push tests need a checksum not present in 'SimpleText' codec
@LuceneTestCase.SuppressCodecs("SimpleText")
public class CorePusherTest extends ZeroStoreSolrCloudTestCase {

  private ZeroCollectionTestUtil collectionUtil;

  @After
  public void after() throws Exception {
    shutdownCluster();
  }

  @SuppressWarnings("try")
  @Test
  public void testExecutionInfo() throws Exception {
    setupCluster(1);
    collectionUtil = new ZeroCollectionTestUtil(cluster, random());

    JettySolrRunner solrRunner = cluster.getJettySolrRunner(0);
    // Inject the CorePushPullFactory to reduce the hard delete delay to 0.
    collectionUtil.createCollection(1);

    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionUtil.COLLECTION_NAME);
    CoreContainer cc = solrRunner.getCoreContainer();

    // Index and commit to create a segment.
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "foo");

    try (SolrCore core = cc.getCore(collection.getReplicas().get(0).getCoreName())) {

      AddUpdateCommand cmd =
          new AddUpdateCommand(new LocalSolrQueryRequest(core, new ModifiableSolrParams()));
      cmd.solrDoc = doc;
      core.getUpdateHandler().addDoc(cmd);
      core.getUpdateHandler()
          .commit(
              new CommitUpdateCommand(
                  new LocalSolrQueryRequest(core, new ModifiableSolrParams()), false));
      ZeroStoreManager storeManager = cc.getZeroStoreManager();

      // Let's take the pull read lock so we can push
      try (AutoCloseable ignore =
          storeManager
              .getMetadataCacheManager()
              .getOrCreateCoreMetadata(core.getName())
              .getZeroAccessLocks()
              .acquireIndexingLock(5)) {
        // First exec should push one segment to the Zero store
        CorePusherExecutionInfo executionInfo =
            storeManager.newCorePusher(core).endToEndPushCoreToZeroStore();

        assertTrue(executionInfo.hasPushed());
        assertNotNull(executionInfo.getMetadataSuffix());
        assertEquals(-1, executionInfo.getZeroGeneration());
        assertTrue(executionInfo.getNumFilesPushed() > 0);
        assertEquals(0, executionInfo.getNumFilesToDelete());

        // Second exec should be noop
        executionInfo = storeManager.newCorePusher(core).endToEndPushCoreToZeroStore();

        assertFalse(executionInfo.hasPushed());
        assertNull(executionInfo.getMetadataSuffix());
        assertEquals(2, executionInfo.getZeroGeneration());
        assertEquals(0, executionInfo.getNumFilesPushed());
        assertEquals(0, executionInfo.getNumFilesToDelete());
      } finally {
        core.getUpdateHandler().close();
      }
    }
  }

  /*
   * Test that if a core is not found in the core container when pushing, an exception is thrown
   */
  @Test
  public void testPushFailsOnMissingCore() {
    assumeWorkingMockito();

    // We can pass null values here because we don't expect those arguments to be interacted with.
    // If they are, a bug is introduced and should be addressed
    CorePusher corePusher =
        new CorePusher(null, null, null, null, null) {
          @Override
          public void enqueueForHardDelete(ZeroStoreShardMetadata shardMetadata) {}
        };

    // verify an exception is thrown
    try {
      corePusher.pushFilesToZeroStore(null);
      fail("pushFilesToZeroStore should have thrown an exception");
    } catch (Exception ex) {
      // core missing from core container should throw exception
    }
  }

  /*
   * Test that pushing to Zero store is successful when shard metadata on the Zero store is empty
   * (or equivalently, didn't exist).
   */
  @Test
  public void testPushSucceedsOnEmptyMetadata() throws Exception {
    assumeWorkingMockito();
    deleteCore();
    initCore("solrconfig.xml", "schema-minimal.xml");

    DeleteProcessor deleteProcessor = Mockito.mock(DeleteProcessor.class);
    ZeroStoreClient zeroStoreClient = setupLocalZeroStoreClient(new ZeroConfig());

    String collectionName = "collectionTest";
    String shardName = "shardTest";

    SolrCore core = h.getCore();

    // add a doc
    String docId = "docID";
    assertU(adoc("id", docId));
    assertU(commit());

    // the doc should be present
    assertQ(req("*:*"), "//*[@numFound='1']");

    // do a push via CorePushPull
    ZeroStoreShardMetadata returnedZcm =
        doPush(core, collectionName, shardName, deleteProcessor, zeroStoreClient);

    // verify the return ZCM is correct
    IndexCommit indexCommit = core.getDeletionPolicy().getLatestCommit();

    // the shardMetadata on Zero store was empty so the file count should be equal to the count of
    // the core's
    // latest commit point
    assertEquals(indexCommit.getFileNames().size(), returnedZcm.getZeroFiles().size());

    // this is a little ugly but the readability is better than the alternatives
    Set<String> zeroFileNames =
        returnedZcm.getZeroFiles().stream()
            .map(ZeroFile.WithLocal::getSolrFileName)
            .collect(Collectors.toSet());
    assertEquals(new HashSet<>(indexCommit.getFileNames()), zeroFileNames);
    deleteCore();
    zeroStoreClient.shutdown();
  }
}
