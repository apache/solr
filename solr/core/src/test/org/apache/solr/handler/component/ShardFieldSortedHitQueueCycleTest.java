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
package org.apache.solr.handler.component;

import java.util.List;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class ShardFieldSortedHitQueueCycleTest extends SolrTestCase {

  @Test
  public void testDocIdLeadingSortHasNoCycle() {
    ShardFieldSortedHitQueue queue =
        new ShardFieldSortedHitQueue(
            new SortField[] {SortField.FIELD_DOC, new SortField("price", SortField.Type.LONG)},
            10,
            null);

    NamedList<List<Object>> shard1Values = new NamedList<>();
    shard1Values.add("price", List.of(5L, 1L));
    NamedList<List<Object>> shard2Values = new NamedList<>();
    shard2Values.add("price", List.of(3L));

    ShardDoc a1 = shardDoc("shard1", 0, shard1Values);
    ShardDoc a2 = shardDoc("shard1", 1, shard1Values);
    ShardDoc b1 = shardDoc("shard2", 0, shard2Values);

    assertFalse(isCycle(queue, a1, a2, b1) || isCycle(queue, a1, b1, a2));
  }

  private static boolean isCycle(
      ShardFieldSortedHitQueue queue, ShardDoc x, ShardDoc y, ShardDoc z) {
    return queue.lessThan(x, y) && queue.lessThan(y, z) && queue.lessThan(z, x);
  }

  private static ShardDoc shardDoc(String shard, int orderInShard, NamedList<List<Object>> values) {
    ShardDoc doc = new ShardDoc();
    doc.shard = shard;
    doc.orderInShard = orderInShard;
    doc.sortFieldValues = values;
    return doc;
  }
}
