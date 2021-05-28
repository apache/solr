package org.apache.solr.handler.component;

import org.apache.lucene.search.SortField;
import org.apache.solr.search.AbstractReRankQuery;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.SortSpec;

/**
 * This class is used to managed the possible multiple SortedHitQueues that we need during mergeIds( ).
 * Multiple queues are needed, if reRanking is used.
 *
 * If reRanking is disabled, only the queue is used.
 * If reRanking is enabled, the top reRankDocsSize documents are added to the reRankQueue, all other documents are
 * collected in the queue.
 */
public class SortedHitQueueManager {
  private final ShardFieldSortedHitQueue queue;
  private final ShardFieldSortedHitQueue reRankQueue;
  private final int reRankDocsSize;

  public SortedHitQueueManager(SortField[] sortFields, SortSpec ss, ResponseBuilder rb) {
    final RankQuery rankQuery = rb.getRankQuery();

    if(rb.getRankQuery() != null && rankQuery instanceof AbstractReRankQuery){
      // reRanking is enabled, create a queue that is only used for reRanked results
      // disable shortcut in the queue to correctly sort documents that were reRanked on the shards back into the full result
      reRankDocsSize = ((AbstractReRankQuery) rankQuery).getReRankDocs();
      reRankQueue = new ShardFieldSortedHitQueue(new SortField[]{SortField.FIELD_SCORE},
          Math.min(reRankDocsSize, ss.getCount()), rb.req.getSearcher());
      queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount(), rb.req.getSearcher(), false);
    } else {
      // reRanking is disabled, use one queue for all results
      queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount(), rb.req.getSearcher());
      reRankQueue = null;
      reRankDocsSize = 0;
    }
  }

  public void addDocument(ShardDoc shardDoc, int i) {
    if(reRankQueue != null && i < reRankDocsSize) {
      ShardDoc droppedShardDoc = reRankQueue.insertWithOverflow(shardDoc);
      // FIXME: Only works if the original request does not sort by score
      if(droppedShardDoc != null) {
        queue.insertWithOverflow(droppedShardDoc);
      }
    } else {
      queue.insertWithOverflow(shardDoc);
    }
  }

  public ShardDoc nextDocument() {
    ShardDoc shardDoc = queue.pop();
    if(shardDoc == null && reRankQueue != null) {
      shardDoc = reRankQueue.pop();
    }
    return shardDoc;
  }

  public int getResultSize(int offset) {
    if(reRankQueue != null) {
      return queue.size() - offset + reRankQueue.size();
    }
    return queue.size() - offset;
  }
}
