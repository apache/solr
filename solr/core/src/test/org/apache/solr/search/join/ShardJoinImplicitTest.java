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

import org.apache.solr.common.SolrInputDocument;
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
  }

  @Test
  public void testScore() throws Exception {
    // without score
    testJoins(toColl, fromColl, "checkRouterField=false", true);
  }

  @Test
  public void testNoScore() throws Exception {
    // with score
    testJoins(toColl, fromColl, "checkRouterField=false", false);
  }
}
