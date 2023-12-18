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

package org.apache.solr.search.join;

import java.util.HashMap;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShardJoinCompositeTest extends ShardToShardJoinAbstract {

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupCluster( // collocate child to parent
        createChild -> {},
        createParent -> {},
        parent -> new SolrInputDocument(),
        (child, parent) -> {
          final SolrInputDocument childDoc = new SolrInputDocument();
          childDoc.setField("id", parent + "!" + child);

          return childDoc;
        });
    final CollectionAdminResponse process =
        CollectionAdminRequest.collectionStatus(toColl).process(cluster.getSolrClient());
    // System.out.println(process);
    {
      final CollectionAdminRequest.Create wrongRouter =
          CollectionAdminRequest.createCollection("wrongRouter", "_default", 3, 2)
              .setProperties(new HashMap<>());
      wrongRouter.setRouterName(ImplicitDocRouter.NAME);
      wrongRouter.setShards("shard1,shard2,shard3");
      wrongRouter.process(cluster.getSolrClient());
    }
    {
      final CollectionAdminRequest.Create wrongShards =
          CollectionAdminRequest.createCollection("wrongShards", "_default", 5, 2)
              .setProperties(new HashMap<>());
      wrongShards.process(cluster.getSolrClient());
    }
  }

  @Test
  public void testScore() throws Exception {
    // without score
    testJoins(toColl, fromColl, "checkRouterField=false", true);
  }

  @Test(expected = SolrException.class)
  public void testScoreFailOnFieldCheck() throws Exception {
    try {
      testJoins(toColl, fromColl, random().nextBoolean() ? "checkRouterField=true" : "", true);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("id"));
      assertTrue(e.getMessage().contains("field"));
      assertTrue(e.getMessage().contains("parent_id_s"));
      throw e;
    }
  }

  @Test(expected = SolrException.class)
  public void testWrongRouter() throws Exception {
    final String fromQ = "name_sI:" + 1011;
    final String joinQ =
        "{!join "
            + "from=parent_id_s fromIndex="
            + "wrongRouter" // fromColl
            + " "
            + " to=id}"
            + fromQ;
    QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "*"));
    CloudSolrClient client = cluster.getSolrClient();
    try {
      client.request(qr);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("router"));
      assertTrue(e.getMessage().contains(toColl));
      assertTrue(e.getMessage().contains("wrongRouter"));
      throw e;
    }
  }

  @Test(expected = SolrException.class)
  public void testWrongShards() throws Exception {
    final String fromQ = "name_sI:" + 1011;
    final String joinQ =
        "{!join "
            + "from=parent_id_s fromIndex="
            + "wrongShards" // fromColl
            + " "
            + " to=id}"
            + fromQ;
    QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "*"));
    CloudSolrClient client = cluster.getSolrClient();
    try {
      client.request(qr);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("hash"));
      assertTrue(e.getMessage().contains("range"));
      throw e;
    }
  }

  @Test(expected = SolrException.class)
  public void testWrongRouterBack() throws Exception {
    final String fromQ = "name_sI:" + 1011;
    final String joinQ =
        "{!join " + "from=parent_id_s fromIndex=" + fromColl + " " + " to=id}" + fromQ;
    QueryRequest qr = new QueryRequest(params("collection", "wrongRouter", "q", joinQ, "fl", "*"));
    CloudSolrClient client = cluster.getSolrClient();
    try {
      client.request(qr);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("router"));
      assertTrue(e.getMessage().contains(fromColl));
      assertTrue(e.getMessage().contains("wrongRouter"));
      throw e;
    }
  }

  @Test
  public void testNoScore() throws Exception {
    testJoins(toColl, fromColl, "checkRouterField=false", false);
  }

  @Test(expected = SolrException.class)
  public void testNoScoreFailOnFieldCheck() throws Exception {
    try {
      testJoins(toColl, fromColl, random().nextBoolean() ? "checkRouterField=true" : "", false);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("id"));
      assertTrue(e.getMessage().contains("field"));
      assertTrue(e.getMessage().contains("parent_id_s"));
      throw e;
    }
  }
}
