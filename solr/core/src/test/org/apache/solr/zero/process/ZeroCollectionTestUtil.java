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

import static org.apache.lucene.tests.util.LuceneTestCase.expectThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.client.api.model.ErrorInfo;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;

public class ZeroCollectionTestUtil {

  public final String COLLECTION_NAME = "zeroCol" + UUID.randomUUID();
  public final String SHARD_NAME = "shard" + UUID.randomUUID();
  private final MiniSolrCloudCluster cluster;
  private final Random random;

  private int docId;

  public ZeroCollectionTestUtil(MiniSolrCloudCluster cluster, Random random) {
    this.cluster = cluster;
    this.random = random;
  }

  public void createCollection(int numReplicas) throws Exception {
    ZeroStoreSolrCloudTestCase.setupZeroCollectionWithShardNames(
        COLLECTION_NAME, numReplicas, SHARD_NAME);
  }

  public void addDocAndCommit() throws SolrServerException, IOException {
    addDocAndCommit(false);
  }

  public void addDocAndCommit(boolean optimize) throws SolrServerException, IOException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", Integer.toString(docId++));
    doc.setField("content", TestUtil.randomSimpleString(random));
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    if (optimize) {
      req.setAction(AbstractUpdateRequest.ACTION.OPTIMIZE, true, true);
    }
    req.commit(cluster.getSolrClient(), COLLECTION_NAME);
  }

  public Replica getFollowerReplica() {
    ClusterState clusterState = cluster.getZkStateReader().getClusterState();
    DocCollection collection = clusterState.getCollection(COLLECTION_NAME);
    Replica leaderReplica = collection.getLeader(SHARD_NAME);
    Replica follower = null;
    for (Replica replica : collection.getReplicas()) {
      if (!replica.getName().equals(leaderReplica.getName())) {
        follower = replica;
        break;
      }
    }
    return follower;
  }

  public static BaseHttpSolrClient.RemoteSolrException expectThrowsRemote(
      Class<? extends Exception> expectedRootExceptionClass,
      SolrException.ErrorCode expectedErrorCode,
      LuceneTestCase.ThrowingRunnable runnable) {
    BaseHttpSolrClient.RemoteSolrException e =
        expectThrows(BaseHttpSolrClient.RemoteSolrException.class, runnable);
    assertEquals(expectedErrorCode.code, e.code());
    assertEquals(expectedRootExceptionClass.getName(), e.getMetadata(ErrorInfo.ROOT_ERROR_CLASS));
    return e;
  }

  public static void expectThrowsRemote(
      Class<? extends Exception> expectedRootExceptionClass,
      SolrException.ErrorCode expectedErrorCode,
      String expectedMessagePart,
      LuceneTestCase.ThrowingRunnable runnable) {
    Exception e = expectThrowsRemote(expectedRootExceptionClass, expectedErrorCode, runnable);
    assertTrue(
        "Expected exception message not found (\"" + expectedMessagePart + "\")",
        e.getMessage().contains(expectedMessagePart));
  }
}
