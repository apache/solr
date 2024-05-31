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

package org.apache.solr.search.combining;

import static org.apache.solr.common.params.CombinerParams.RECIPROCAl_RANK_FUSION;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Combining considers two or more query rankedLists: resultA, resultB ...<br>
 * For a given query, each query result is a ranked list of documents La = (a1,a2,...), Lb = (b1,
 * b2, ...)...<br>
 * A combining algorithm creates a unique ranked list I = (i1, i2, ...).<br>
 * This list is created by combining elements from the lists la and lb as described by the
 * implementation algorithm.<br>
 */
public abstract class QueriesCombiner {

  protected int upTo;

  public QueriesCombiner(SolrParams requestParams) {
    this.upTo =
        requestParams.getInt(CombinerParams.COMBINER_UP_TO, CombinerParams.COMBINER_UP_TO_DEFAULT);
  }

  public abstract QueryResult combine(QueryResult[] rankedLists);

  QueryResult initCombinedResult(QueryResult[] rankedLists) {
    QueryResult combinedRankedList = new QueryResult();
    boolean partialResults = false;
    for (QueryResult result : rankedLists) {
      partialResults |= result.isPartialResults();
    }
    combinedRankedList.setPartialResults(partialResults);

    boolean segmentTerminatedEarly = false;
    for (QueryResult result : rankedLists) {
      if (result.getSegmentTerminatedEarly() != null) {
        segmentTerminatedEarly |= result.getSegmentTerminatedEarly();
      }
    }
    combinedRankedList.setSegmentTerminatedEarly(segmentTerminatedEarly);

    combinedRankedList.setNextCursorMark(rankedLists[0].getNextCursorMark());
    return combinedRankedList;
  }

  public abstract NamedList<Explanation> getExplanations(
      String[] queryKeys,
      List<Query> queries,
      List<DocList> resultsPerQuery,
      SolrIndexSearcher searcher,
      IndexSchema schema)
      throws IOException;

  public static QueriesCombiner getImplementation(SolrParams requestParams) {
    String algorithm = requestParams.get(CombinerParams.COMBINER_ALGORITHM, RECIPROCAl_RANK_FUSION);
    switch (algorithm) {
      case RECIPROCAl_RANK_FUSION:
        return new ReciprocalRankFusion(requestParams);
      default:
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Unknown Combining algorithm: " + algorithm);
    }
  }
}
