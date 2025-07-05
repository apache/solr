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
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * The QueryAndResponseCombiner class is an abstract base class for combining query results and
 * responses. It provides a framework for different algorithms to be implemented for merging ranked
 * lists and shard documents.
 */
public abstract class QueryAndResponseCombiner {

  protected int upTo;

  /**
   * Constructs a QueryAndResponseCombiner instance.
   *
   * @param requestParams the SolrParams containing the request parameters.
   */
  protected QueryAndResponseCombiner(SolrParams requestParams) {
    this.upTo =
        requestParams.getInt(CombinerParams.COMBINER_UP_TO, CombinerParams.COMBINER_UP_TO_DEFAULT);
  }

  /**
   * Combines multiple ranked lists into a single QueryResult.
   *
   * @param rankedLists a list of ranked lists to be combined
   * @return a new QueryResult containing the combined results
   * @throws IllegalArgumentException if the input list is empty
   */
  public abstract QueryResult combine(List<QueryResult> rankedLists);

  /**
   * Combines shard documents based on the provided map.
   *
   * @param shardDocMap a map where keys represent shard IDs and values are lists of ShardDocs for
   *     each shard
   * @return a combined list of ShardDocs from all shards
   */
  public abstract List<ShardDoc> combine(Map<String, List<ShardDoc>> shardDocMap);

  /**
   * Retrieves a list of explanations for the given queries and results.
   *
   * @param queryKeys the keys associated with the queries
   * @param queries the list of queries for which explanations are requested
   * @param resultsPerQuery the list of document lists corresponding to each query
   * @param searcher the SolrIndexSearcher used to perform the search
   * @param schema the IndexSchema used to interpret the search results
   * @return a list of explanations for the given queries and results
   * @throws IOException if an I/O error occurs during the explanation retrieval process
   */
  public abstract NamedList<Explanation> getExplanations(
      String[] queryKeys,
      List<Query> queries,
      List<DocList> resultsPerQuery,
      SolrIndexSearcher searcher,
      IndexSchema schema)
      throws IOException;

  /**
   * Retrieves an implementation of the QueryAndResponseCombiner based on the specified algorithm.
   *
   * @param requestParams the SolrParams containing the request parameters, including the combiner
   *     algorithm.
   * @return an instance of QueryAndResponseCombiner corresponding to the specified algorithm.
   * @throws SolrException if an unknown combiner algorithm is specified.
   */
  public static QueryAndResponseCombiner getImplementation(SolrParams requestParams) {
    String algorithm =
        requestParams.get(CombinerParams.COMBINER_ALGORITHM, CombinerParams.RECIPROCAL_RANK_FUSION);
    if (algorithm.equals(CombinerParams.RECIPROCAL_RANK_FUSION)) {
      return new ReciprocalRankFusion(requestParams);
    }
    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST, "Unknown Combining algorithm: " + algorithm);
  }
}
