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
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.junit.BeforeClass;
import org.junit.Test;

/** use router.field to collocate children to parent */
public class ShardJoinImplicitTest extends ShardToShardJoinAbstract {

  private static String[] shards = new String[] {"a", "b", "c", "d", "e"};

  @BeforeClass
  public static void setupCluster() throws Exception {

    setupCluster( // collocate child to parent
        createChild -> {
          createChild.setRouterName(ImplicitDocRouter.NAME);
          createChild.setShards(String.join(",", shards));
        },
        createParent -> {
          createParent.setRouterName(ImplicitDocRouter.NAME);
          createParent.setShards(String.join(",", shards));
        },
        parent -> {
          final SolrInputDocument parentDoc = new SolrInputDocument();
          parentDoc.setField("_route_", shards[(parent.hashCode()) % shards.length]);
          return parentDoc;
        },
        (child, parent) -> {
          final SolrInputDocument childDoc = new SolrInputDocument();
          childDoc.setField("id", child);
          childDoc.setField("_route_", shards[parent.hashCode() % shards.length]);
          return childDoc;
        });
    final CollectionAdminRequest.Create wrongRouter =
        CollectionAdminRequest.createCollection("wrongRouter", "_default", 3, 2)
            .setProperties(new HashMap<>());
    wrongRouter.setRouterName(CompositeIdRouter.NAME);
    wrongRouter.process(cluster.getSolrClient());
  }

  @Test
  public void testScore() throws Exception {
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
