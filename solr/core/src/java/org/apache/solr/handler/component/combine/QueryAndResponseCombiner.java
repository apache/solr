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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.search.CollapsingQParserPlugin.CollapsingPostFilter;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrDefaultScorerSupplier;
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
   * @param collapseFilter the collapse post filter, or null if no collapse dedup is needed
   * @param searcher the searcher to read field values from, required when collapseFilter is
   *     non-null
   * @return the combined query result
   */
  public static QueryResult simpleCombine(
      List<QueryResult> queryResults,
      CollapsingPostFilter collapseFilter,
      SolrIndexSearcher searcher) {
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

    // If a collapse field is specified, deduplicate by field value across combined queries.
    // Each sub-query already collapsed individually, but different sub-queries may have
    // selected different group heads for the same field value.
    int removedByCollapse = 0;
    if (collapseFilter != null && searcher != null && queryResults.size() > 1) {
      int preCollapseSize = uniqueDocIds.size();
      combinedDocSet =
          removeCollapsedDuplicatesViaSearcher(
              collapseFilter, searcher, uniqueDocIds, combinedDocSet);
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
   * Removes collapsed duplicates by delegating to SolrIndexSearcher with the CollapsingPostFilter.
   * Uses a PrecomputedScoreQuery to preserve original scores from combined sub-queries, then
   * applies the collapse filter to determine surviving doc IDs.
   *
   * <p>This leverages Solr's native collapse infrastructure instead of manually reading DocValues.
   */
  private static DocSet removeCollapsedDuplicatesViaSearcher(
      CollapsingPostFilter collapseFilter,
      SolrIndexSearcher searcher,
      Map<Integer, Float> uniqueDocIds,
      DocSet combinedDocSet) {
    try {
      QueryCommand cmd =
          new QueryCommand()
              .setQuery(new PrecomputedScoreQuery(uniqueDocIds))
              .setFilterList(List.of(combinedDocSet.makeQuery(), collapseFilter))
              .setLen(uniqueDocIds.size())
              .setNeedDocSet(true);
      QueryResult result = searcher.search(cmd);

      Set<Integer> survivingDocIds = new HashSet<>();
      DocIterator iter = result.getDocList().iterator();
      while (iter.hasNext()) {
        survivingDocIds.add(iter.nextDoc());
      }
      uniqueDocIds.keySet().retainAll(survivingDocIds);
      return result.getDocSet();
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
   * A query that returns pre-computed scores for specific doc IDs. Used to preserve original scores
   * from combined sub-queries when delegating collapse to the searcher.
   */
  private static class PrecomputedScoreQuery extends Query {
    private final Map<Integer, Float> docScores;

    PrecomputedScoreQuery(Map<Integer, Float> docScores) {
      this.docScores = docScores;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      return new PrecomputedScoreWeight(this, docScores, boost);
    }

    @Override
    public String toString(String field) {
      return "PrecomputedScoreQuery(docs=" + docScores.size() + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
      visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof PrecomputedScoreQuery otherQuery
          && docScores.equals(otherQuery.docScores);
    }

    @Override
    public int hashCode() {
      return classHash() * 31 + docScores.hashCode();
    }
  }

  private static class PrecomputedScoreWeight extends Weight {
    private final Map<Integer, Float> docScores;
    private final float boost;

    PrecomputedScoreWeight(Query query, Map<Integer, Float> docScores, float boost) {
      super(query);
      this.docScores = docScores;
      this.boost = boost;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) {
      int globalDoc = context.docBase + doc;
      Float score = docScores.get(globalDoc);
      if (score != null) {
        return Explanation.match(score * boost, "precomputed score");
      }
      return Explanation.noMatch("no precomputed score");
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) {
      List<Integer> localDocs = getLocalDocs(context);
      if (localDocs.isEmpty()) {
        return null;
      }
      Scorer scorer = new PrecomputedScoreScorer(localDocs, context.docBase, docScores, boost);
      return new SolrDefaultScorerSupplier(scorer);
    }

    private List<Integer> getLocalDocs(LeafReaderContext context) {
      List<Integer> localDocs = new ArrayList<>();
      int maxDoc = context.reader().maxDoc();
      for (int globalDoc : docScores.keySet()) {
        int local = globalDoc - context.docBase;
        if (local >= 0 && local < maxDoc) {
          localDocs.add(local);
        }
      }
      Collections.sort(localDocs);
      return localDocs;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class PrecomputedScoreScorer extends Scorer {
    private final List<Integer> localDocs;
    private final int docBase;
    private final Map<Integer, Float> docScores;
    private final float boost;
    private int idx = -1;

    PrecomputedScoreScorer(
        List<Integer> localDocs, int docBase, Map<Integer, Float> docScores, float boost) {
      this.localDocs = localDocs;
      this.docBase = docBase;
      this.docScores = docScores;
      this.boost = boost;
    }

    @Override
    public DocIdSetIterator iterator() {
      return new DocIdSetIterator() {
        @Override
        public int docID() {
          return PrecomputedScoreScorer.this.docID();
        }

        @Override
        public int nextDoc() {
          idx++;
          return docID();
        }

        @Override
        public int advance(int target) {
          do {
            idx++;
          } while (idx < localDocs.size() && localDocs.get(idx) < target);
          return docID();
        }

        @Override
        public long cost() {
          return localDocs.size();
        }
      };
    }

    @Override
    public float getMaxScore(int upTo) {
      return Float.MAX_VALUE;
    }

    @Override
    public float score() {
      Float s = docScores.get(localDocs.get(idx) + docBase);
      return (s != null ? s : 0f) * boost;
    }

    @Override
    public int docID() {
      if (idx < 0) {
        return -1;
      }
      return idx < localDocs.size() ? localDocs.get(idx) : NO_MORE_DOCS;
    }
  }
}
