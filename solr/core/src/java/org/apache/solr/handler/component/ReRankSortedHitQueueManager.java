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
 * This class is used to manage the multiple SortedHitQueues that we need during {@link QueryComponent#mergeIds(ResponseBuilder, ShardRequest)}.
 * Multiple queues are needed when reRanking is used.
 *
 * The top reRankDocsSize documents are added to the reRankQueue, all other documents are
 * collected in the queue.
 */
public class ReRankSortedHitQueueManager extends SortedHitQueueManager {

  private final ShardFieldSortedHitQueue queue;
  private final ShardFieldSortedHitQueue reRankQueue;
  private final int reRankDocsSize;

  public ReRankSortedHitQueueManager(SortField[] sortFields, int count, int offset, IndexSearcher searcher, int reRankDocsSize) {
      this.reRankDocsSize = reRankDocsSize;
      int absoluteReRankDocs = Math.min(reRankDocsSize, count);
      reRankQueue = new ShardFieldSortedHitQueue(new SortField[]{SortField.FIELD_SCORE},
              absoluteReRankDocs, searcher);
      queue = new ShardFieldSortedHitQueue(sortFields, offset + count - absoluteReRankDocs,
              searcher, false /* useSameShardShortcut */);
  }

  /**
   * Check whether the sortFields contain score.
   * If that's true, we do not support this query. (see comments below)
   * @deprecated will be removed as soon as https://github.com/apache/solr/pull/171 is done
   */
  @Deprecated // will be removed when all sort fields become supported
  public static boolean supports(SortField[] sortFields) {
    for (SortField sortField : sortFields) {
      // do not check equality with SortField.FIELD_SCORE because that would only cover score desc, not score asc
      if (sortField.getType().equals(SortField.Type.SCORE) && sortField.getField() == null) {
        // using two queues makes reRanking on Solr Cloud possible (@see https://github.com/apache/solr/pull/151 )
        // however, the fix does not work if the non-reRanked docs should be sorted by score ( @see https://github.com/apache/solr/pull/151#discussion_r640664451 )
        // to maintain the existing behavior for these cases, we do not support the broken use of two queues and
        // continue to use the (also broken) status quo instead
        return false;
      }
    }
    return true;
  }

  public void addDocument(ShardDoc shardDoc) {
    if(shardDoc.orderInShard < reRankDocsSize) {
      ShardDoc droppedShardDoc = reRankQueue.insertWithOverflow(shardDoc);
      // Only works if the original request does not sort by score
      // see https://issues.apache.org/jira/browse/SOLR-15437
      if(droppedShardDoc != null) {
        queue.insertWithOverflow(droppedShardDoc);
      }
    } else {
      queue.insertWithOverflow(shardDoc);
    }
  }

  public ShardDoc popDocument() {
    ShardDoc shardDoc = queue.pop();
    if(shardDoc == null) {
      shardDoc = reRankQueue.pop();
    }
    return shardDoc;
  }

  public int size() {
    return queue.size() + reRankQueue.size();
  }

}
