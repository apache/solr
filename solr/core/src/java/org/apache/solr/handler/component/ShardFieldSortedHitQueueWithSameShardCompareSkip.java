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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;

/**
 * Used by distributed search to merge results.
 * If two documents are on the same shard, their orderInShard is used for sorting instead of comparing their sort values.
 * This shortcut does only work if we are not reRanking the results.
 */
public class ShardFieldSortedHitQueueWithSameShardCompareSkip extends ShardFieldSortedHitQueue {

  public ShardFieldSortedHitQueueWithSameShardCompareSkip(SortField[] fields, int size, IndexSearcher searcher) {
    super(fields, size, searcher);
  }

  @Override
  protected boolean lessThan(ShardDoc docA, ShardDoc docB) {
    // If these docs are from the same shard, then the relative order
    // is how they appeared in the response from that shard.    
    if (docA.shard == docB.shard) {
      // if docA has a smaller position, it should be "larger" so it
      // comes before docB.
      // This will handle sorting by docid within the same shard

      // comment this out to test comparators.
      return !(docA.orderInShard < docB.orderInShard);
    }
    
    return super.lessThan(docA, docB);
  }
}