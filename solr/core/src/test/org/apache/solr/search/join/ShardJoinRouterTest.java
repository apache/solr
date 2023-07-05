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
import org.junit.BeforeClass;
import org.junit.Test;

/** use router.field to collocate children to parent */
public class ShardJoinRouterTest extends ShardToShardJoinAbstract {

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupCluster( // collocate child to parent
        createChild -> createChild.setRouterField("parent_id_s"),
        createParent -> {},
        parent -> new SolrInputDocument(),
        (child, parent) -> {
          final SolrInputDocument childDoc = new SolrInputDocument();
          childDoc.setField("id", child);
          return childDoc;
        });
  }

  @Test
  public void testScore() throws Exception {
    testJoins(toColl, fromColl, "", true);
  }

  @Test
  public void testNoScore() throws Exception {
    testJoins(toColl, fromColl, "", false);
  }
}
