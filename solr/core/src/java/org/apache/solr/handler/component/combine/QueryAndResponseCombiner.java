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
package org.apache.solr.handler.component.combine;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntDoubleHashMap;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * The QueryAndResponseCombiner class is an abstract base class for combining query results and
 * shard documents. It provides a framework for different algorithms to be implemented for merging
 * ranked lists and shard documents.
 */
public abstract class QueryAndResponseCombiner implements NamedListInitializedPlugin {
  /**
   * Combines shard documents corresponding to multiple queries based on the provided map.
   *
   * @param queriesDocMap a map where keys represent combiner query keys and values are lists of
   *     ShardDocs for corresponding to each key
   * @param solrParams params to be used when provided at query time
   * @return a combined list of ShardDocs from all queries
   */
  public abstract List<ShardDoc> combine(
      Map<String, List<ShardDoc>> queriesDocMap, SolrParams solrParams);

  /**
   * Combine query result list as a union, optionally deduplicating by a collapse field. When a
   * collapse filter is provided, only one document per unique field value is kept (based on the
   * collapse sort/score selection). This ensures that collapse semantics are preserved across
   * combined queries.
   *
   * @param queryResults the query results to be combined
   * @param collapseFilters the collapse post filters, or empty if no collapse dedup is needed
   * @param searcher the searcher to read field values from, required when collapseFilters is
   *     non-empty
   * @return the combined query result
   */
  public static QueryResult simpleCombine(
      List<QueryResult> queryResults, List<Query> collapseFilters, SolrIndexSearcher searcher) {
    QueryResult combinedQueryResults = new QueryResult();
    DocSet combinedDocSet = null;
    Map<Integer, Float> uniqueDocIds = new HashMap<>();
    long totalMatches = 0;
    for (QueryResult queryResult : queryResults) {
      DocIterator docs = queryResult.getDocList().iterator();
      totalMatches = Math.max(totalMatches, queryResult.getDocList().matches());
      while (docs.hasNext()) {
        uniqueDocIds.put(docs.nextDoc(), queryResult.getDocList().hasScores() ? docs.score() : 0f);
      }
      if (combinedDocSet == null) {
        combinedDocSet = queryResult.getDocSet();
      } else if (queryResult.getDocSet() != null) {
        combinedDocSet = combinedDocSet.union(queryResult.getDocSet());
      }
    }

    // If collapse fields are specified, deduplicate by field value across combined queries.
    // Each sub-query already collapsed individually, but different sub-queries may have
    // selected different group heads for the same field value.
    int removedByCollapse = 0;
    if (CollectionUtil.isNotEmpty(collapseFilters) && searcher != null && queryResults.size() > 1) {
      int preCollapseSize = uniqueDocIds.size();
      combinedDocSet =
          removeCollapsedDuplicatesViaSearcher(
              collapseFilters, searcher, uniqueDocIds, combinedDocSet);
      removedByCollapse = preCollapseSize - uniqueDocIds.size();
    }

    int combinedResultsLength = uniqueDocIds.size();
    int[] combinedResultsDocIds = new int[combinedResultsLength];
    float[] combinedResultScores = new float[combinedResultsLength];

    int i = 0;
    for (Map.Entry<Integer, Float> scoredDoc : uniqueDocIds.entrySet()) {
      combinedResultsDocIds[i] = scoredDoc.getKey();
      combinedResultScores[i] = scoredDoc.getValue();
      i++;
    }
    DocSlice combinedResultSlice =
        new DocSlice(
            0,
            combinedResultsLength,
            combinedResultsDocIds,
            combinedResultScores,
            Math.max(combinedResultsLength, totalMatches - removedByCollapse),
            combinedResultScores.length > 0 ? combinedResultScores[0] : 0,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    combinedQueryResults.setDocList(combinedResultSlice);
    combinedQueryResults.setDocSet(combinedDocSet);
    return combinedQueryResults;
  }

  /**
   * Removes collapsed duplicates across combined sub-queries. Ensures that only one document per
   * collapse field value retained across the merged results. Entries removed by collapsing are also
   * removed from {@code uniqueDocIds} (mutated in place).
   *
   * @return the collapsed combined DocSet, or null if combinedDocSet was null
   */
  private static DocSet removeCollapsedDuplicatesViaSearcher(
      List<Query> collapseFilters,
      SolrIndexSearcher searcher,
      Map<Integer, Float> uniqueDocIds,
      DocSet combinedDocSet) {
    IntDoubleHashMap scoreMap = new IntDoubleHashMap(uniqueDocIds.size());
    uniqueDocIds.forEach((doc, score) -> scoreMap.put(doc, score.doubleValue()));
    Query baseQuery;
    boolean needDocSet;
    if (combinedDocSet != null) {
      baseQuery = combinedDocSet.makeQuery();
      needDocSet = true;
    } else {
      int[] queryDocIds =
          uniqueDocIds.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
      baseQuery = new SortedIntDocSet(queryDocIds).makeQuery();
      needDocSet = false;
    }
    Query scoredQuery =
        FunctionScoreQuery.boostByValue(baseQuery, new PrecomputedScoreValuesSource(scoreMap));

    try {
      QueryCommand cmd =
          new QueryCommand()
              .setQuery(scoredQuery)
              .setFilterList(collapseFilters)
              .setLen(uniqueDocIds.size())
              .setNeedDocSet(needDocSet);
      QueryResult result = searcher.search(cmd);

      Set<Integer> retainedDocIds = HashSet.newHashSet(result.getDocList().size());
      DocIterator iter = result.getDocList().iterator();
      while (iter.hasNext()) {
        retainedDocIds.add(iter.nextDoc());
      }

      uniqueDocIds.keySet().retainAll(retainedDocIds);
      return needDocSet ? result.getDocSet() : null;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Retrieves a list of explanations for the given queries and results.
   *
   * @param queryKeys the keys associated with the queries
   * @param queriesDocMap a map where keys represent combiner query keys and values are lists of
   *     ShardDocs for corresponding to each key
   * @param combinedQueriesDocs a list of ShardDocs after combiner operation
   * @param solrParams params to be used when provided at query time
   * @return a SimpleOrderedMap of explanations for the given queries and results
   */
  public abstract SimpleOrderedMap<Explanation> getExplanations(
      String[] queryKeys,
      Map<String, List<ShardDoc>> queriesDocMap,
      List<ShardDoc> combinedQueriesDocs,
      SolrParams solrParams);

  /**
   * Retrieves an implementation of the QueryAndResponseCombiner based on the specified algorithm.
   *
   * @param algorithm the combiner algorithm
   * @param combiners The already initialised map of QueryAndResponseCombiner
   * @return an instance of QueryAndResponseCombiner corresponding to the specified algorithm.
   * @throws SolrException if an unknown combiner algorithm is specified.
   */
  public static QueryAndResponseCombiner getImplementation(
      String algorithm, Map<String, QueryAndResponseCombiner> combiners) {
    if (combiners.get(algorithm) != null) {
      return combiners.get(algorithm);
    }
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST, "Unknown Combining algorithm: " + algorithm);
  }

  /**
   * A {@link DoubleValuesSource} backed by a global doc ID to score map. Returns pre-computed
   * scores for specific document IDs.
   */
  private static class PrecomputedScoreValuesSource extends DoubleValuesSource {

    private final IntDoubleHashMap scoreByDoc;

    PrecomputedScoreValuesSource(IntDoubleHashMap scoreByDoc) {
      this.scoreByDoc = scoreByDoc;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues existing) {
      int base = ctx.docBase;
      return new DoubleValues() {
        private double currentScore;

        @Override
        public double doubleValue() {
          return currentScore;
        }

        @Override
        public boolean advanceExact(int doc) {
          int globalDoc = base + doc;
          currentScore = scoreByDoc.get(globalDoc);
          return true;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher searcher) {
      return this;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof PrecomputedScoreValuesSource other && scoreByDoc.equals(other.scoreByDoc);
    }

    @Override
    public int hashCode() {
      return scoreByDoc.hashCode();
    }

    @Override
    public String toString() {
      return "PrecomputedScoreValuesSource(docs=" + scoreByDoc.size() + ")";
    }
  }
}
