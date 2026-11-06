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
package org.apache.solr.update;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.update.SolrCmdDistributor.SolrError;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SOLR-5939: when the streaming client merges several requests into one stream and that stream
 * fails, the error must be recorded against every request that was in the stream. Stamping it with
 * the first request ever sent to the node makes the distributor retry, exempt, and account the
 * wrong request.
 *
 * <p>Both tests send requests to a node where nothing listens, so every stream fails with a
 * connection error, and then inspect the errors the distributor collected.
 */
public class StreamingSolrClientsErrorAttributionTest extends SolrTestCase {

  private UpdateShardHandler updateShardHandler;

  @BeforeClass
  public static void beforeClass() {
    // keep the client's merge window short so a drained stream closes quickly
    System.setProperty("solr.cloud.client.pollQueueTime", "500");
  }

  @AfterClass
  public static void afterClass() {
    System.clearProperty("solr.cloud.client.pollQueueTime");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    updateShardHandler = new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (updateShardHandler != null) {
      updateShardHandler.close();
    }
    super.tearDown();
  }

  private static StdNode deadNode(int maxRetries) throws IOException {
    int port;
    try (ServerSocket socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      port = socket.getLocalPort();
    }
    Map<String, Object> props = new HashMap<>();
    props.put("base_url", "http://127.0.0.1:" + port + "/solr");
    props.put("core", "collection1");
    props.put("node_name", "127.0.0.1:" + port + "_solr");
    props.put("type", "NRT");
    props.put("state", "active");
    Replica replica = new Replica("core_node1", props, "collection1", "shard1");
    return new StdNode(new ZkCoreNodeProps(replica), "collection1", "shard1", maxRetries);
  }

  private static AddUpdateCommand addCmd(String id) {
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    cmd.solrDoc = doc;
    return cmd;
  }

  @Test
  public void testMergedStreamErrorIsAttributedToEveryRequestInTheStream() throws Exception {
    StdNode node = deadNode(0);
    try (SolrCmdDistributor distrib = new SolrCmdDistributor(updateShardHandler)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      distrib.distribAdd(addCmd("1"), List.of(node), params);
      distrib.distribAdd(addCmd("2"), List.of(node), params);
      distrib.finish();

      List<SolrError> errors = distrib.getErrors();
      Set<String> errorDocIds = new HashSet<>();
      for (SolrError error : errors) {
        assertEquals(1, error.req.uReq.getDocuments().size());
        errorDocIds.add(error.req.uReq.getDocuments().get(0).getFieldValue("id").toString());
      }
      assertEquals("errors: " + errors, Set.of("1", "2"), errorDocIds);
      assertEquals("errors: " + errors, 2, errors.size());
    }
  }

  @Test
  public void testDeleteByQueryRetryRuleIsJudgedOnItsOwnRequest() throws Exception {
    StdNode node = deadNode(1);
    try (SolrCmdDistributor distrib = new SolrCmdDistributor(updateShardHandler)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      DeleteUpdateCommand delete = new DeleteUpdateCommand(null);
      delete.setQuery("id:1");
      distrib.distribDelete(delete, List.of(node), params);
      distrib.distribAdd(addCmd("3"), List.of(node), params);
      distrib.finish();

      List<SolrError> errors = distrib.getErrors();
      assertEquals("errors: " + errors, 2, errors.size());
      SolrError deleteError = null;
      SolrError addError = null;
      for (SolrError error : errors) {
        if (error.req.uReq.getDeleteQuery() != null && !error.req.uReq.getDeleteQuery().isEmpty()) {
          deleteError = error;
        } else {
          addError = error;
        }
      }
      assertNotNull("the delete-by-query must carry its own error", deleteError);
      assertEquals(0, deleteError.req.retries);
      assertNotNull("the add in the same stream must carry its own error", addError);
      assertEquals("3", addError.req.uReq.getDocuments().get(0).getFieldValue("id"));
      assertEquals(
          "the add is retriable, so it must have been resubmitted once", 1, addError.req.retries);
    }
  }
}
