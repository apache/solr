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
package org.apache.solr.crossdc.handler;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrKafkaTestsIgnoredThreadsFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ThreadLeakFilters(
    defaultFilters = true,
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      SolrKafkaTestsIgnoredThreadsFilter.class
    })
public class MirroringCollectionsHandlerTest extends SolrTestCaseJ4 {

  private final KafkaMirroringSink sink = Mockito.mock(KafkaMirroringSink.class);
  private final CoreContainer coreContainer = Mockito.mock(CoreContainer.class);
  private final ZkController zkController = Mockito.mock(ZkController.class);
  private final SolrZkClient solrZkClient = Mockito.mock(SolrZkClient.class);

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<MirroredSolrRequest<?>> captor;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    Mockito.when(coreContainer.isZooKeeperAware()).thenReturn(true);
    Mockito.when(coreContainer.getZkController()).thenReturn(zkController);
    Mockito.when(zkController.getZkClient()).thenReturn(solrZkClient);
    Mockito.doAnswer(inv -> null)
        .when(solrZkClient)
        .getData(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());
    captor = ArgumentCaptor.forClass(MirroredSolrRequest.class);
    Mockito.doNothing().when(sink).submit(captor.capture());
    // make ConfUtil happy
    System.setProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS, "foo");
    System.setProperty(KafkaCrossDcConf.TOPIC_NAME, "foo");
  }

  @After
  public void teardown() throws Exception {
    System.clearProperty(KafkaCrossDcConf.MIRROR_COLLECTIONS);
    System.clearProperty(KafkaCrossDcConf.BOOTSTRAP_SERVERS);
    System.clearProperty(KafkaCrossDcConf.TOPIC_NAME);
    super.tearDown();
  }

  @Test
  public void testAllCollections() throws Exception {
    for (String collection : List.of("test1", "test2", "test3")) {
      CollectionAdminRequest.Create create =
          CollectionAdminRequest.createCollection(collection, "_default", 2, 1, 1, 1);
      SolrParams params = create.getParams();
      runCommand(params, true);
    }
  }

  @Test
  public void testSomeCollections() throws Exception {
    System.setProperty(KafkaCrossDcConf.MIRROR_COLLECTIONS, "test1,test2");
    for (String collection : List.of("test1", "test2", "test3")) {
      CollectionAdminRequest.Create create =
          CollectionAdminRequest.createCollection(collection, "_default", 2, 1, 1, 1);
      SolrParams params = create.getParams();
      runCommand(params, !collection.equals("test3"));
    }
    List<MirroredSolrRequest<?>> mirroredSolrRequests = captor.getAllValues();
    assertEquals(2, mirroredSolrRequests.size());
    assertEquals("test1", mirroredSolrRequests.get(0).getSolrRequest().getParams().get("name"));
    assertEquals("test2", mirroredSolrRequests.get(1).getSolrRequest().getParams().get("name"));
  }

  @Test
  public void testOtherCommands() throws Exception {
    CollectionAdminRequest<?> req =
        CollectionAdminRequest.addReplicaToShard("test", "shard1", Replica.Type.NRT);
    runCommand(req.getParams(), true);

    req = CollectionAdminRequest.deleteReplica("test", "shard1", 1);
    runCommand(req.getParams(), true);

    req = CollectionAdminRequest.deleteCollection("test");
    runCommand(req.getParams(), true);
  }

  private void runCommand(SolrParams params, boolean expectResult) throws Exception {
    int initialMirroredCount = captor.getAllValues().size();
    MirroringCollectionsHandler handler =
        Mockito.spy(new MirroringCollectionsHandler(coreContainer, sink));
    Mockito.doNothing().when(handler).baseHandleRequestBody(Mockito.any(), Mockito.any());
    SolrQueryRequest req = new LocalSolrQueryRequest(null, params);
    SolrQueryResponse rsp = new SolrQueryResponse();
    handler.handleRequestBody(req, rsp);
    if (expectResult) {
      assertEquals(
          "should capture additional mirrored value",
          initialMirroredCount + 1,
          captor.getAllValues().size());
      MirroredSolrRequest<?> mirroredSolrRequest = captor.getValue();
      assertNotNull("missing mirrored request", mirroredSolrRequest);
      assertEquals(MirroredSolrRequest.Type.ADMIN, mirroredSolrRequest.getType());
      SolrRequest<?> solrRequest = mirroredSolrRequest.getSolrRequest();
      SolrParams mirroredParams = solrRequest.getParams();
      params.forEach(
          entry -> {
            assertEquals(entry.getValue(), mirroredParams.getParams(entry.getKey()));
          });
    } else {
      assertEquals(initialMirroredCount, captor.getAllValues().size());
    }
  }

  @Test
  public void testCoreContainerInit() throws Exception {
    Path home = createTempDir();
    String solrXml = IOUtils.resourceToString("/mirroring-solr.xml", StandardCharsets.UTF_8);
    CoreContainer cores = new CoreContainer(SolrXmlConfig.fromString(home, solrXml));
    try {
      cores.load();
      assertTrue(cores.getCollectionsHandler() instanceof MirroringCollectionsHandler);
    } finally {
      cores.shutdown();
    }
  }
}
