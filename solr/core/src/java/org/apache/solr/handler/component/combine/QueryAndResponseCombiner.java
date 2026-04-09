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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
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
   * collapse field is provided, only one document per unique field value is kept (the one with the
   * highest score). This ensures that collapse semantics are preserved across combined queries.
   *
   * @param queryResults the query results to be combined
   * @param collapseField the field to collapse on, or null if no collapse dedup is needed
   * @param searcher the searcher to read field values from, required when collapseField is non-null
   * @return the combined query result
   */
  public static QueryResult simpleCombine(
      List<QueryResult> queryResults, SchemaField collapseField, SolrIndexSearcher searcher) {
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
    if (collapseField != null && searcher != null && queryResults.size() > 1) {
      int preCollapseSize = uniqueDocIds.size();
      combinedDocSet =
          removeCollapsedDuplicates(collapseField, searcher, uniqueDocIds, combinedDocSet);
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

  private static DocSet removeCollapsedDuplicates(
      SchemaField collapseField,
      SolrIndexSearcher searcher,
      Map<Integer, Float> uniqueDocIds,
      DocSet combinedDocSet) {
    try {
      List<Integer> removedDocs = collapseByFieldValue(uniqueDocIds, collapseField, searcher);
      if (!removedDocs.isEmpty()) {
        for (int docId : removedDocs) {
          uniqueDocIds.remove(docId);
        }
        if (combinedDocSet != null) {
          FixedBitSet bits = new FixedBitSet(searcher.maxDoc());
          for (int docId : removedDocs) {
            bits.set(docId);
          }
          return combinedDocSet.andNot(new BitDocSet(bits, removedDocs.size()));
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return combinedDocSet;
  }

  /**
   * Deduplicates the doc map by collapse field value. For each unique field value, only the doc
   * with the highest score is retained. Docs with null/missing field values are kept (null policy
   * pass).
   *
   * @return list of doc IDs that should be removed
   */
  private static List<Integer> collapseByFieldValue(
      Map<Integer, Float> uniqueDocIds, SchemaField collapseField, SolrIndexSearcher searcher)
      throws IOException {
    Map<Integer, Object> docFieldValues = readDocValues(uniqueDocIds, collapseField, searcher);
    if (docFieldValues.isEmpty()) {
      return List.of();
    }
    return findDuplicatesByFieldValue(uniqueDocIds, docFieldValues);
  }

  /**
   * Reads collapse field values from DocValues for the given doc IDs. Entries are accessed in doc
   * ID order as required by the forward-only DocValues API.
   */
  private static Map<Integer, Object> readDocValues(
      Map<Integer, Float> uniqueDocIds, SchemaField collapseField, SolrIndexSearcher searcher)
      throws IOException {
    LeafReader reader = searcher.getSlowAtomicReader();
    FieldType fieldType = collapseField.getType();
    String fieldName = collapseField.getName();

    List<Integer> sortedDocIds = new java.util.ArrayList<>(uniqueDocIds.keySet());
    java.util.Collections.sort(sortedDocIds);

    Map<Integer, Object> docFieldValues = new HashMap<>();
    if (fieldType instanceof StrField) {
      SortedDocValues sdv = DocValues.getSorted(reader, fieldName);
      for (int docId : sortedDocIds) {
        if (sdv.advanceExact(docId)) {
          docFieldValues.put(docId, sdv.ordValue());
        }
      }
    } else if (fieldType.getNumberType() != null) {
      NumericDocValues ndv = DocValues.getNumeric(reader, fieldName);
      for (int docId : sortedDocIds) {
        if (ndv.advanceExact(docId)) {
          docFieldValues.put(docId, ndv.longValue());
        }
      }
    }
    return docFieldValues;
  }

  /**
   * Finds duplicate docs by field value, keeping the highest-scoring doc per unique value. Docs
   * without a field value (null policy) are not considered duplicates.
   *
   * @return list of doc IDs that are duplicates and should be removed
   */
  private static List<Integer> findDuplicatesByFieldValue(
      Map<Integer, Float> uniqueDocIds, Map<Integer, Object> docFieldValues) {
    Map<Object, Map.Entry<Integer, Float>> bestPerGroup = new HashMap<>();
    List<Integer> docsToRemove = new ArrayList<>();

    for (Map.Entry<Integer, Float> entry : uniqueDocIds.entrySet()) {
      Object fieldVal = docFieldValues.get(entry.getKey());
      if (fieldVal == null) {
        continue;
      }
      Map.Entry<Integer, Float> existing = bestPerGroup.get(fieldVal);
      if (existing == null) {
        bestPerGroup.put(fieldVal, entry);
      } else if (entry.getValue() > existing.getValue()) {
        docsToRemove.add(existing.getKey());
        bestPerGroup.put(fieldVal, entry);
      } else {
        docsToRemove.add(entry.getKey());
      }
    }
    return docsToRemove;
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
}
