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
package org.apache.solr.cloud;

import static org.apache.solr.client.solrj.response.RequestStatusState.COMPLETED;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.util.SecurityJson;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecoveryZkTestWithAuth extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    cluster =
        configureCluster(2)
            .addConfig("conf", configset("cloud-minimal"))
            .withSecurityJson(SecurityJson.SIMPLE)
            .configure();
  }

  private <T extends SolrRequest<? extends SolrResponse>> T withBasicAuth(T req) {
    req.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
    return req;
  }

  private QueryResponse queryWithBasicAuth(SolrClient client, SolrQuery q)
      throws IOException, SolrServerException {
    return withBasicAuth(new QueryRequest(q)).process(client);
  }

  @Test
  public void testRecoveryWithAuthEnabled() throws Exception {
    final String collection = "recoverytestwithauth";
    withBasicAuth(CollectionAdminRequest.createCollection(collection, "conf", 1, 1))
        .process(cluster.getSolrClient());
    waitForState(
        "Expected a collection with one shard and one replicas", collection, clusterShape(1, 1));
    try (SolrClient solrClient =
        cluster.basicSolrClientBuilder().withDefaultCollection(collection).build()) {
      UpdateRequest commitReq = new UpdateRequest();
      withBasicAuth(commitReq);
      for (int i = 0; i < 500; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        String[] fields = {"id", i + "", "name", "name = " + i};
        for (int j = 0; j < fields.length; j += 2) {
          doc.addField(fields[j], fields[j + 1]);
        }
        UpdateRequest req = new UpdateRequest();
        withBasicAuth(req).add(doc);
        req.process(solrClient, collection);
        if (i % 10 == 0) {
          commitReq.commit(solrClient, collection);
        }
      }
      commitReq.commit(solrClient, collection);

      withBasicAuth(CollectionAdminRequest.addReplicaToShard(collection, "shard1"));
      CollectionAdminRequest.AddReplica addReplica =
          CollectionAdminRequest.addReplicaToShard(collection, "shard1");
      withBasicAuth(addReplica);
      RequestStatusState status = addReplica.processAndWait(collection, solrClient, 120);
      assertEquals(COMPLETED, status);
      cluster
          .getZkStateReader()
          .waitForState(collection, 120, TimeUnit.SECONDS, clusterShape(1, 2));
      DocCollection state = getCollectionState(collection);
      assertShardConsistency(state.getSlice("shard1"), true);
    }
  }

  private void assertShardConsistency(Slice shard, boolean expectDocs) throws Exception {
    List<Replica> replicas = shard.getReplicas(r -> r.getState() == Replica.State.ACTIVE);
    long[] numCounts = new long[replicas.size()];
    int i = 0;
    for (Replica replica : replicas) {
      try (var client =
          new HttpSolrClient.Builder(replica.getBaseUrl())
              .withDefaultCollection(replica.getCoreName())
              .withHttpClient(((CloudLegacySolrClient) cluster.getSolrClient()).getHttpClient())
              .build()) {
        var q = new SolrQuery("*:*");
        q.add("distrib", "false");
        numCounts[i] = queryWithBasicAuth(client, q).getResults().getNumFound();
        i++;
      }
    }
    for (int j = 1; j < replicas.size(); j++) {
      if (numCounts[j] != numCounts[j - 1])
        fail("Mismatch in counts between replicas"); // TODO improve this!
      if (numCounts[j] == 0 && expectDocs)
        fail("Expected docs on shard " + shard.getName() + " but found none");
    }
  }
}
