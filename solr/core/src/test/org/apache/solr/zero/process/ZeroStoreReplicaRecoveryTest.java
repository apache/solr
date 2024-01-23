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

import static org.apache.solr.common.params.CommonParams.JAVABIN;
import static org.apache.solr.handler.ReplicationHandler.CMD_INDEX_VERSION;
import static org.apache.solr.handler.ReplicationHandler.COMMAND;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.handler.ReplicationHandler;

/** Unit tests around replicas of {@link Type#ZERO} recovering */
public class ZeroStoreReplicaRecoveryTest extends ZeroStoreSolrCloudTestCase {

  /*
   * Verify that replication is disabled for ZERO replicas - replication is disabled
   * by rejecting any request for ZERO replicas to the replication handler
   */
  public void testZeroReplicaReplicationDisabled() throws Exception {
    setupCluster(1);
    String collectionName = "zeroCollection";
    setupZeroCollectionWithShardNames(collectionName, 1, "shard1");
    DocCollection collection =
        cluster.getSolrClient().getClusterStateProvider().getCollection(collectionName);
    Replica replica = collection.getReplicas().get(0);

    try (SolrClient replicaDirectClient =
        getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getCoreName())) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(COMMAND, CMD_INDEX_VERSION);
      params.set(CommonParams.WT, JAVABIN);
      params.set(CommonParams.QT, ReplicationHandler.PATH);
      QueryRequest req = new QueryRequest(params);
      try {
        replicaDirectClient.request(req);
        fail("Replication request expected to fail for ZERO replica");
      } catch (SolrException ex) {
        assertEquals(ErrorCode.BAD_REQUEST.code, ex.code());
        assertTrue(ex.getMessage().contains("ZERO replicas should never peer replicate"));
      }
    }
  }
}
