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
package org.apache.solr.search.combine;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.IndexSchema;
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
   * Combines multiple ranked lists into a single QueryResult.
   *
   * @param rankedLists a list of ranked lists to be combined
   * @param solrParams params to be used when provided at query time
   * @return a new QueryResult containing the combined results
   * @throws IllegalArgumentException if the input list is empty
   */
  public abstract QueryResult combine(List<QueryResult> rankedLists, SolrParams solrParams);

  /**
   * Combines shard documents based on the provided map.
   *
   * @param shardDocMap a map where keys represent shard IDs and values are lists of ShardDocs for
   *     each shard
   * @param solrParams params to be used when provided at query time
   * @return a combined list of ShardDocs from all shards
   */
  public abstract List<ShardDoc> combine(
      Map<String, List<ShardDoc>> shardDocMap, SolrParams solrParams);

  /**
   * Retrieves a list of explanations for the given queries and results.
   *
   * @param queryKeys the keys associated with the queries
   * @param queries the list of queries for which explanations are requested
   * @param queryResult the list of QueryResult corresponding to each query
   * @param searcher the SolrIndexSearcher used to perform the search
   * @param schema the IndexSchema used to interpret the search results
   * @param solrParams params to be used when provided at query time
   * @return a list of explanations for the given queries and results
   * @throws IOException if an I/O error occurs during the explanation retrieval process
   */
  public abstract NamedList<Explanation> getExplanations(
      String[] queryKeys,
      List<Query> queries,
      List<QueryResult> queryResult,
      SolrIndexSearcher searcher,
      IndexSchema schema,
      SolrParams solrParams)
      throws IOException;

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
