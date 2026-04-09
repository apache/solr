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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
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
import org.apache.solr.search.CollapsingQParserPlugin.CollapsingPostFilter;
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
      SchemaField collapseField = searcher.getSchema().getField(collapseFilter.getField());
      combinedDocSet =
          removeCollapsedDuplicates(
              collapseField, collapseFilter, searcher, uniqueDocIds, combinedDocSet);
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
      CollapsingPostFilter collapseFilter,
      SolrIndexSearcher searcher,
      Map<Integer, Float> uniqueDocIds,
      DocSet combinedDocSet) {
    try {
      List<Integer> removedDocs =
          collapseByFieldValue(uniqueDocIds, collapseField, collapseFilter, searcher);
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
   * Deduplicates the doc map by collapse field value. For each unique field value, the winning doc
   * is determined by the collapse selection strategy: sort-based collapse uses the sort spec,
   * score-based collapse keeps the highest-scoring doc. Docs with null/missing field values are
   * kept (null policy pass).
   *
   * @return list of doc IDs that should be removed
   */
  private static List<Integer> collapseByFieldValue(
      Map<Integer, Float> uniqueDocIds,
      SchemaField collapseField,
      CollapsingPostFilter collapseFilter,
      SolrIndexSearcher searcher)
      throws IOException {
    Map<Integer, Object> docFieldValues =
        readDocValues(uniqueDocIds.keySet(), collapseField, searcher);
    if (docFieldValues.isEmpty()) {
      return List.of();
    }

    // Build a comparator based on the collapse selection strategy
    Comparator<Map.Entry<Integer, ?>> sortComparator =
        buildSortComparator(collapseFilter, uniqueDocIds, searcher);

    return findDuplicatesByFieldValue(uniqueDocIds, docFieldValues, sortComparator);
  }

  /**
   * Reads field values from DocValues for a given set of doc IDs. Entries are accessed in doc ID
   * order as required by the forward-only DocValues API.
   *
   * @param docIds the doc IDs to read values for
   * @param field the schema field to read
   * @param searcher the searcher providing the index reader
   * @return map of docId to field value (ordinal for strings, long for numerics)
   */
  private static Map<Integer, Object> readDocValues(
      Iterable<Integer> docIds, SchemaField field, SolrIndexSearcher searcher) throws IOException {
    LeafReader reader = searcher.getSlowAtomicReader();
    FieldType fieldType = field.getType();
    String fieldName = field.getName();

    List<Integer> sortedDocIds = new ArrayList<>();
    docIds.forEach(sortedDocIds::add);
    Collections.sort(sortedDocIds);

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
   * Builds a comparator that determines which doc wins per group based on the collapse selection
   * strategy. For sort-based collapse, pre-reads the sort field values and compares by them. For
   * score-based collapse (default), compares by score.
   */
  private static Comparator<Map.Entry<Integer, ?>> buildSortComparator(
      CollapsingPostFilter collapseFilter,
      Map<Integer, Float> uniqueDocIds,
      SolrIndexSearcher searcher)
      throws IOException {
    if (collapseFilter.getSortSpec() != null) {
      Sort sort = collapseFilter.getSortSpec().getSort();
      if (sort != null && sort.getSort().length > 0) {
        Comparator<Map.Entry<Integer, ?>> chained = null;
        for (SortField sortField : sort.getSort()) {
          Comparator<Map.Entry<Integer, ?>> fieldCmp =
              buildSingleFieldComparator(sortField, uniqueDocIds, searcher);
          chained = (chained == null) ? fieldCmp : chained.thenComparing(fieldCmp);
        }
        if (chained != null) {
          return chained;
        }
      }
    }
    // Default: score-based (higher score wins)
    return scoreComparator(true);
  }

  /** Builds a score-based comparator. Higher score wins when preferHigher=true. */
  @SuppressWarnings("unchecked")
  private static Comparator<Map.Entry<Integer, ?>> scoreComparator(boolean preferHigher) {
    Comparator<Map.Entry<Integer, ?>> cmp =
        Comparator.comparing(e -> ((Map.Entry<Integer, Float>) e).getValue());
    return preferHigher ? cmp : cmp.reversed();
  }

  /** Builds a comparator for a single SortField — handles both score and schema fields. */
  private static Comparator<Map.Entry<Integer, ?>> buildSingleFieldComparator(
      SortField sortField, Map<Integer, Float> uniqueDocIds, SolrIndexSearcher searcher)
      throws IOException {
    String sortFieldName = sortField.getField();
    if (sortFieldName == null) {
      // Score field: reverse=false means natural (desc), reverse=true means asc
      return scoreComparator(!sortField.getReverse());
    }
    SchemaField sf = searcher.getSchema().getFieldOrNull(sortFieldName);
    if (sf != null) {
      Map<Integer, Object> sortValues = readDocValues(uniqueDocIds.keySet(), sf, searcher);
      return fieldValueComparator(sortValues, sortField.getReverse());
    }
    // Unknown field — neutral comparator
    return (a, b) -> 0;
  }

  /** Builds a comparator over pre-read field values. Higher value wins when preferHigher=true. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Comparator<Map.Entry<Integer, ?>> fieldValueComparator(
      Map<Integer, Object> fieldValues, boolean preferHigher) {
    Comparator cmp =
        preferHigher
            ? Comparator.nullsFirst(Comparator.naturalOrder())
            : Comparator.nullsFirst(Comparator.reverseOrder());
    return (a, b) -> cmp.compare(fieldValues.get(a.getKey()), fieldValues.get(b.getKey()));
  }

  /**
   * Finds duplicate docs by field value, keeping the winning doc per unique value based on the
   * provided comparator. Docs without a field value (null policy) are not considered duplicates.
   *
   * @return list of doc IDs that are duplicates and should be removed
   */
  private static List<Integer> findDuplicatesByFieldValue(
      Map<Integer, Float> uniqueDocIds,
      Map<Integer, Object> docFieldValues,
      Comparator<Map.Entry<Integer, ?>> sortComparator) {
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
      } else if (sortComparator.compare(entry, existing) > 0) {
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
