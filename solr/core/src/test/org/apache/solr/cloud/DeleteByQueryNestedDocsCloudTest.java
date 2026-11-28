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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

/** Delete-by-query deletes nested descendants consistently on every replica. */
public class DeleteByQueryNestedDocsCloudTest extends SolrCloudTestCase {

  private static final String COLLECTION = "nested_dbq";
  private static final int NUM_TREES = 10;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @Test
  public void testReplicasConsistent() throws Exception {
    final SolrClient client = cluster.getSolrClient();
    final Set<String> expected = new HashSet<>();
    final UpdateRequest add = new UpdateRequest();
    for (int t = 0; t < NUM_TREES; t++) {
      add.add(tree("T" + t, expected));
    }
    add.commit(client, COLLECTION);
    assertEquals(NUM_TREES * 7, expected.size());
    assertEquals(expected, allIds());

    // roots, a mid-level child, and a leaf
    new UpdateRequest().deleteByQuery("id:(T0 T1 T2.0 T3.1.1)").commit(client, COLLECTION);
    expected.removeIf(id -> id.startsWith("T0") || id.startsWith("T1") || id.startsWith("T2.0"));
    expected.remove("T3.1.1");

    // deleting by a field on children: the "subs" docs at /kids/subs
    new UpdateRequest()
        .deleteByQuery("_nest_path_:\\/kids\\/subs AND _root_:T4")
        .commit(client, COLLECTION);
    expected.removeIf(id -> id.startsWith("T4.") && id.length() > "T4.0".length());

    assertEquals(expected, allIds());
    for (Slice slice : cluster.getSolrClient().getClusterState().getCollection(COLLECTION)) {
      Set<String> leaderIds = null;
      for (Replica replica : slice.getReplicas()) {
        final Set<String> ids = replicaIds(replica);
        if (leaderIds == null) {
          leaderIds = ids;
        } else {
          assertEquals("replica " + replica.getName(), leaderIds, ids);
        }
      }
    }
  }

  /** A root with 2 children, each with 2 children: 7 docs in total. */
  private static SolrInputDocument tree(String id, Set<String> ids) {
    final SolrInputDocument root = new SolrInputDocument("id", id);
    ids.add(id);
    final List<SolrInputDocument> kids = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      final String kidId = id + "." + i;
      final SolrInputDocument kid = new SolrInputDocument("id", kidId);
      final List<SolrInputDocument> subs = new ArrayList<>();
      for (int j = 0; j < 2; j++) {
        subs.add(new SolrInputDocument("id", kidId + "." + j));
        ids.add(kidId + "." + j);
      }
      kid.setField("subs", subs);
      kids.add(kid);
      ids.add(kidId);
    }
    root.setField("kids", kids);
    return root;
  }

  private static Set<String> allIds() throws Exception {
    return ids(cluster.getSolrClient(), COLLECTION, new SolrQuery("*:*"));
  }

  private static Set<String> replicaIds(Replica replica) throws Exception {
    final SolrQuery query = new SolrQuery("*:*");
    query.set("distrib", false);
    return ids(cluster.getReplicaJetty(replica).getSolrClient(), replica.getCoreName(), query);
  }

  private static Set<String> ids(SolrClient client, String collection, SolrQuery query)
      throws Exception {
    query.setFields("id");
    query.setRows(NUM_TREES * 7);
    final Set<String> ids = new HashSet<>();
    for (SolrDocument doc : client.query(collection, query).getResults()) {
      ids.add((String) doc.getFieldValue("id"));
    }
    return ids;
  }
}
