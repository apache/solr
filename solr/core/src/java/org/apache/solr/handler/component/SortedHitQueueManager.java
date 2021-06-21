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

import org.apache.lucene.search.SortField;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.SortSpec;

public abstract class SortedHitQueueManager {

  abstract void addDocument(ShardDoc shardDoc);

  abstract ShardDoc popDocument();

  abstract int size();

  /**
   * Return the implementation of SortedHitQueueManager that should be used for the given sort and ranking.
   * We use multiple queues if reRanking is enabled and we do not sort by score.
   * In all other cases only one queue is used.
   *
   * @param sortFields the fields by which the results should be sorted
   * @param ss SortSpec of the responseBuilder
   * @param rb responseBuilder for a query
   * @return either a SortedHitQueueManager that handles reRanking or the default implementation
   */
  public static SortedHitQueueManager newSortedHitQueueManager(SortField[] sortFields, SortSpec ss, ResponseBuilder rb) {
    final RankQuery rankQuery = rb.getRankQuery();
    if (rankQuery instanceof AbstractReRankQuery && ReRankSortedHitQueueManager.supports(sortFields)) {
      return new ReRankSortedHitQueueManager(sortFields, ss.getCount(), ss.getOffset(), rb.req.getSearcher(),
          ((AbstractReRankQuery)rankQuery).getReRankDocs());
    } else {
      return new DefaultSortedHitQueueManager(sortFields, ss.getCount(), ss.getOffset(), rb.req.getSearcher());
    }
  }
}